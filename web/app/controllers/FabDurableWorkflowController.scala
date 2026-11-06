package controllers

import akka.actor.ActorSystem
import akka.actor.typed.{ActorRef, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.util.{ByteString, Timeout}
import javax.inject.{Inject, Singleton}
import net.imadz.application.durable.{FabDurableWorkflowProgram, FabLedgerClient, FabWorkflowModel}
import net.imadz.monarch.persistence.batch.{BatchProtocol, WorkflowHost}
import play.api.Configuration
import play.api.inject.ApplicationLifecycle
import play.api.libs.json.{JsObject, JsString, Json}
import play.api.mvc._
import play.filters.csrf.{CSRF, CSRFAddToken}

import java.nio.file.Paths
import java.util.concurrent.{RejectedExecutionException, TimeoutException}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** HTTP admission and committed queries only. WorkflowHost owns the entire durable workflow;
  * this controller has no job registry, progress cache, scheduler or equipment state machine.
  */
@Singleton
final class FabDurableWorkflowController @Inject() (
    cc: ControllerComponents,
    config: Configuration,
    actorSystem: ActorSystem,
    lifecycle: ApplicationLifecycle,
    addCsrfToken: CSRFAddToken) extends AbstractController(cc) {
  private implicit val ec: ExecutionContext = actorSystem.dispatcher
  private val typedSystem = actorSystem.toTyped
  private implicit val scheduler: Scheduler = typedSystem.scheduler
  private implicit val timeout: Timeout = Timeout(5.seconds)
  require(Paths.get(config.get[String]("fab.durable.storage-root")).isAbsolute,
    "fab.durable.storage-root must be an absolute, exclusively owned demo directory")
  private val fixtureControls = config.get[Boolean]("fab.durable.fixture-controls")
  private val ledger = new FabLedgerClient(config.get[Int]("fab.durable.fixture-port"))
  lifecycle.addStopHook(() => { ledger.close(); Future.successful(()) })
  private val host = new WorkflowHost(typedSystem, config.get[String]("fab.durable.namespace"),
    new FabDurableWorkflowProgram(ledger), FabWorkflowModel.MaxJobs, FabWorkflowModel.MaxChildren)

  private def local(request: RequestHeader): Boolean =
    Set("127.0.0.1", "::1", "0:0:0:0:0:0:0:1")(request.remoteAddress)
  private def error(message: String): JsObject = Json.obj("error" -> message)
  private def noCache(result: Result): Result = result.withHeaders(CACHE_CONTROL -> "no-store")

  private def protect(request: RequestHeader, jobId: String, controls: Boolean = false)
      (work: => Future[Result]): Future[Result] = {
    val result = if (!local(request)) Future.successful(Forbidden(error("local demo requests only")))
    else if (!FabWorkflowModel.validJobId(jobId))
      Future.successful(BadRequest(error("jobId must match [A-Za-z0-9][A-Za-z0-9_-]{0,47}")))
    else if (controls && !fixtureControls)
      Future.successful(Forbidden(error("external fixture controls are disabled")))
    else try work catch { case NonFatal(cause) => Future.failed(cause) }
    result.recover {
      case _: TimeoutException => GatewayTimeout(error(
        "reply timed out; query committed state, then repeat only the same jobId/count if needed"))
      case _: RejectedExecutionException => ServiceUnavailable(error("bounded fixture I/O capacity reached"))
      case _: IllegalArgumentException => ServiceUnavailable(error("bounded workflow host unavailable"))
      case NonFatal(_) => ServiceUnavailable(error(
        "workflow or fixture unavailable; no success or safe retry is inferred"))
    }.map(noCache)
  }

  private def current(ref: ActorRef[BatchProtocol.Command]): Future[BatchProtocol.Reply] =
    ref.ask[BatchProtocol.Reply](reply => BatchProtocol.Get(reply, limit = FabWorkflowModel.MaxItems))

  private val runtimeOrigin = classOf[WorkflowHost].getProtectionDomain.getCodeSource.getLocation.toExternalForm
  play.api.Logger(getClass).info("DURABLE_RUNTIME_ORIGIN " + runtimeOrigin)

  private def viewJson(view: BatchProtocol.View): JsObject = Json.obj(
    "runtimeOrigin" -> runtimeOrigin,
    "source" -> "WorkflowHost committed BatchProtocol.View",
    "jobId" -> view.jobId, "phase" -> view.phase, "inputClosed" -> view.inputClosed,
    "accepted" -> view.accepted, "succeeded" -> view.succeeded, "failed" -> view.failed,
    "held" -> view.held, "active" -> view.active, "activeBytes" -> view.activeBytes,
    "truncated" -> view.truncated, "reason" -> view.reason,
    "items" -> view.items.map(item => Json.obj(
      "sequence" -> item.sequence, "itemId" -> item.itemId, "area" -> item.area,
      "phase" -> item.phase, "executions" -> item.executions, "reason" -> item.reason,
      "result" -> item.result.utf8String)),
    "batches" -> view.batches.map(batch => Json.obj(
      "batchId" -> batch.batchId, "area" -> batch.area, "phase" -> batch.phase,
      "itemIds" -> batch.itemIds, "reservationId" -> batch.reservationId,
      "deliveryId" -> batch.deliveryId, "reason" -> batch.reason)))

  private def unexpected(reply: BatchProtocol.Reply): Result = reply match {
    case BatchProtocol.Rejected(reason) => Conflict(error(reason))
    case BatchProtocol.Backpressured(reason) => TooManyRequests(error(reason))
    case _ => BadGateway(error("unexpected workflow reply"))
  }

  def start(jobId: String, count: Int): Action[Unit] = Action.async(parse.empty) { request =>
    protect(request, jobId) {
      if (count < 1 || count > FabWorkflowModel.MaxItems)
        Future.successful(BadRequest(error("count must be between 1 and 10")))
      else {
        val ref = host.job(jobId)
        // Sequential, bounded offers; the journal's immutable input digest deduplicates even
        // after CloseInput/restart. Count is part of every offer, not a controller-side cache.
        def offer(sequence: Int, duplicates: Int): Future[Result] = {
          if (sequence == count) ref.ask[BatchProtocol.Reply](BatchProtocol.CloseInput.apply).map {
            case BatchProtocol.Current(view) => Ok(Json.obj(
              "view" -> viewJson(view), "duplicateOffers" -> duplicates, "requestedCount" -> count))
            case other => unexpected(other)
          }
          else {
            val item = FabWorkflowModel.input(jobId, count, sequence)
            ref.ask[BatchProtocol.Reply](reply => BatchProtocol.Offer(item.sequence, item.itemId,
              ByteString(item.payload.toArray), item.area, item.compatibilityKey, reply)).flatMap {
              case BatchProtocol.Accepted(seq, id, duplicate) if seq == item.sequence && id == item.itemId =>
                offer(sequence + 1, duplicates + (if (duplicate) 1 else 0))
              case other => Future.successful(unexpected(other))
            }
          }
        }
        offer(0, 0)
      }
    }
  }

  def state(jobId: String): Action[Unit] = Action.async(parse.empty) { request =>
    protect(request, jobId) {
      current(host.job(jobId)).map {
        case BatchProtocol.Current(view) => Ok(viewJson(view))
        case other => unexpected(other)
      }
    }
  }

  def reconcile(jobId: String, batchId: String): Action[Unit] = Action.async(parse.empty) { request =>
    protect(request, jobId) {
      host.reconcile(jobId, batchId).map {
        case BatchProtocol.Current(view) => Ok(viewJson(view))
        case other => unexpected(other)
      }
    }
  }

  def tasks(jobId: String): Action[Unit] = Action.async(parse.empty) { request =>
    protect(request, jobId, controls = true) {
      // One finite diagnostic call, filtered by parsed identity rather than a prefix match.
      ledger.call("TASKS").map {
        case response if response.headOption.contains("TASKS") && response.size <= 15 &&
            response.tail.distinct.size == response.tail.size =>
          Ok(Json.obj("source" -> "external fixture; not host progress", "jobId" -> jobId,
            "operationKeys" -> response.tail.filter(key => FabWorkflowModel.operationJob(key) == Right(jobId))))
        case Vector("REJECTED", reason) => BadGateway(error("fixture rejected TASKS: " + reason.take(256)))
        case _ => BadGateway(error("malformed fixture TASKS response"))
      }
    }
  }

  def completeTask(jobId: String, operationKey: String): Action[Unit] = Action.async(parse.empty) { request =>
    protect(request, jobId, controls = true) {
      if (FabWorkflowModel.operationJob(operationKey) != Right(jobId))
        Future.successful(BadRequest(error("operationKey must identify a bounded batch of this exact job")))
      else current(host.job(jobId)).flatMap {
        case BatchProtocol.Current(view) => view.batches.find(_.batchId + "/execute" == operationKey) match {
          case None => Future.successful(Conflict(error("operation is not an active committed batch of this job")))
          case Some(batch) => ledger.call("COMPLETE", Vector(operationKey)).map {
            case Vector("SUCCEEDED", taskId, epoch, receipt) if epoch == FabWorkflowModel.LedgerEpoch &&
                taskId.length <= 64 && taskId.matches("task-[1-9][0-9]*") &&
                FabWorkflowModel.parseReceipt(ByteString(receipt)).toOption.exists(r =>
                  r.area == batch.area && r.rows.map(_._1).sorted == batch.itemIds.sorted) =>
              Ok(Json.obj("source" -> "external fixture", "operationKey" -> operationKey,
                "taskId" -> taskId, "receipt" -> receipt,
                "note" -> "external completion only; poll the committed host view for workflow progress"))
            case Vector("REJECTED", reason) => Conflict(error("fixture rejected COMPLETE: " + reason.take(256)))
            case Vector("NOT_FOUND", _) => Conflict(error("fixture history unavailable; do not resubmit as a new operation"))
            case _ => BadGateway(error("malformed or mismatched fixture completion receipt"))
          }
        }
        case other => Future.successful(unexpected(other))
      }
    }
  }

  def index(): Action[AnyContent] = addCsrfToken(Action { implicit request =>
    if (!local(request)) noCache(Forbidden(error("local demo requests only")))
    else noCache(Ok(page(CSRF.getToken(request).map(_.value).getOrElse(""))).as(HTML))
  })

  private def page(csrfToken: String): String = {
    val token = Json.stringify(JsString(csrfToken)).replace("<", "\\u003c")
    s"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fab durable workflow</title><style>
body{font:16px system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;background:#f5f7fa;color:#182537}
input,button{font:inherit;padding:.5rem;margin:.25rem}button{cursor:pointer}pre{background:white;padding:1rem;overflow:auto;border:1px solid #ccd4df}
#tasks button{display:block}small{display:block;margin:1rem 0}
</style></head><body><h1>Fab durable workflow</h1>
<p>每个 item = 一个 FOUP = 一片 wafer。完整流程由 WorkflowHost 执行；页面不维护工作流状态。</p>
<label>稳定 jobId <input id="job" value="fab-demo-01" maxlength="48" pattern="[A-Za-z0-9][A-Za-z0-9_-]{0,47}"></label>
<label>数量 <input id="count" type="number" min="1" max="10" value="2"></label>
<button id="start">提交并显式关闭输入</button><button id="poll">查询 / 继续轮询</button><button id="stop">停止轮询</button>
<small>同一 jobId 必须保持相同 count；重放不会创建新业务尝试。默认两项中的 item-1 会走一次返工。
只以 committed view 为准；外部完成回执不等于整个工作流完成。每次轮询最多 300 轮。</small>
<p id="message" role="status"></p><h2>已提交视图</h2><pre id="view">尚未查询</pre>
<div id="reconcile"></div><small>自动查证有上限。人工按钮只恢复同一操作的有限查证，不强制成功、不换任务重发。</small>
<h2>外部任务</h2><p id="controls"></p><div id="tasks"></div>
<script>
'use strict';
const csrfToken = $token, controlsEnabled = $fixtureControls;
const el = id => document.getElementById(id);
let timer, remaining = 0, busy = false;
function path() {
  const job = el('job').value;
  if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$$/.test(job)) throw new Error('jobId 格式无效');
  return '/fab/durable/jobs/' + encodeURIComponent(job);
}
async function api(url, method = 'GET') {
  const abort = new AbortController(), timeout = setTimeout(() => abort.abort(), method === 'POST' ? 60000 : 7000);
  try {
    const response = await fetch(url, {method, signal: abort.signal, cache: 'no-store', credentials: 'same-origin',
      headers: method === 'POST' ? {'Csrf-Token': csrfToken, 'Content-Type': 'application/json'} : {},
      body: method === 'POST' ? '{}' : undefined});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || ('HTTP ' + response.status));
    return data;
  } finally { clearTimeout(timeout); }
}
async function poll() {
  if (busy || remaining <= 0) return;
  busy = true;
  try {
    const base = path();
    const view = await api(base);
    el('view').textContent = JSON.stringify(view, null, 2);
    el('reconcile').replaceChildren();
    for (const batch of view.batches) {
      const button = document.createElement('button');
      button.textContent = '查证原操作 ' + batch.batchId;
      button.onclick = async () => {
        button.disabled = true;
        try {
          await api(base + '/reconcile?batchId=' + encodeURIComponent(batch.batchId), 'POST');
          el('message').textContent = '已请求有限查证；继续观察已提交状态。';
        } catch (error) { el('message').textContent = error.message; }
        finally { button.disabled = false; }
      };
      el('reconcile').appendChild(button);
    }
    if (controlsEnabled) {
      const data = await api(base + '/tasks');
      el('tasks').replaceChildren();
      for (const key of data.operationKeys) {
        const button = document.createElement('button');
        button.textContent = '完成外部任务 ' + key;
        button.onclick = async () => {
          button.disabled = true;
          try {
            const receipt = await api(base + '/complete?operationKey=' + encodeURIComponent(key), 'POST');
            el('message').textContent = receipt.note;
          } catch (error) { el('message').textContent = error.message; }
          finally { button.disabled = false; }
        };
        el('tasks').appendChild(button);
      }
    }
  } catch (error) { el('message').textContent = error.message; }
  finally {
    busy = false; remaining -= 1;
    if (remaining > 0) timer = setTimeout(poll, 2000);
  }
}
function beginPolling() { clearTimeout(timer); remaining = 300; poll(); }
el('controls').textContent = controlsEnabled ? '只向 127.0.0.1 的独立 fixture 发送单次 TASKS / COMPLETE；不会自行完成任务。' :
  '外部控制默认关闭；由父进程完成设备任务，或显式开启 fab.durable.fixture-controls。';
el('poll').onclick = beginPolling;
el('stop').onclick = () => { remaining = 0; clearTimeout(timer); };
el('start').onclick = async () => {
  const count = Number(el('count').value);
  if (!Number.isInteger(count) || count < 1 || count > 10) { el('message').textContent = 'count 必须为 1..10'; return; }
  el('start').disabled = true;
  try {
    const data = await api(path() + '/start?count=' + count, 'POST');
    el('view').textContent = JSON.stringify(data.view, null, 2);
    el('message').textContent = '输入已关闭；重复 offer 数量 ' + data.duplicateOffers;
    beginPolling();
  } catch (error) { el('message').textContent = error.message + '；可能已有部分输入持久化，只能用相同 jobId/count 重放。'; }
  finally { el('start').disabled = false; }
};
</script></body></html>"""
  }
}
