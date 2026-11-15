package net.imadz.application.chain

import akka.actor.typed.ActorRef
import akka.util.Timeout
import net.imadz.application.actor.EquipmentAreaActor
import net.imadz.application.aggregates.LotProtocol._
import net.imadz.application.chain.FabExecutionModel.{FabDemoContext, FabDemoState, StageError, StageFailedException}
import net.imadz.application.scenario.DecisionConfig
import net.imadz.domain.events._
import net.imadz.fab.protocol._
import net.imadz.monarch.StaleRun
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.control.{NoStackTrace, NonFatal}

/** Shared stage functions. Ownership checks fence in-process continuations;
  * they cannot revoke a command already delivered to an equipment/area actor. */
object PipelineStages {

  private val logger = LoggerFactory.getLogger("PipelineStages")

  /** Admission/transport uncertainty is not an equipment fault and must not be
    * fed to OCAP. The persistent host records controlled recovery for this category;
    * callers must reconcile it before re-executing anonymous jobs.
    * Deliberately NOT StageFailedException (and NOT a fabricated LOT_ACK_UNKNOWN).
    */
  final case class RecoveryRequired(errorCode: String, detail: String, underlying: Throwable = null)
      extends RuntimeException(s"$errorCode: $detail", underlying) with NoStackTrace

  private def requireFresh(ctx: FabDemoContext): Unit =
    if (!ctx.runToken()) throw StaleRun

  private def inRun[A](ctx: FabDemoContext)(body: => Future[A]): Future[A] = {
    if (!ctx.runToken()) Future.failed(StaleRun)
    else try body catch { case NonFatal(ex) => Future.failed(ex) }
  }

  /** Check before the I/O/wait and on BOTH successful and failed replies. */
  private def checked[A](ctx: FabDemoContext)(body: => Future[A]): Future[A] =
    inRun(ctx)(body).transform { result =>
      if (!ctx.runToken()) Failure(StaleRun) else result
    }(ctx.ec)

  private def notifyLot(ctx: FabDemoContext)(command: ActorRef[LotConfirmation] => LotCommand): Unit = {
    requireFresh(ctx)
    ctx.notifyLot(command)
  }

  private def publish(ctx: FabDemoContext, event: FabSimulationEvent): Unit = {
    requireFresh(ctx)
    try ctx.publish(event) catch {
      case ex if ex eq StaleRun => throw StaleRun
      case NonFatal(ex) => logger.warn("Fab event observer failed", ex)
    }
    requireFresh(ctx)
  }

  private def progress(ctx: FabDemoContext, status: String, detail: String, phase: String): Unit = {
    requireFresh(ctx)
    try ctx.stageProgress(status, detail, phase) catch {
      case ex if ex eq StaleRun => throw StaleRun
      case NonFatal(ex) => logger.warn("Fab progress observer failed", ex)
    }
    requireFresh(ctx)
  }

  private def recovery[A](ctx: FabDemoContext, code: String, detail: String,
                          cause: Throwable = null): Future[A] = {
    requireFresh(ctx)
    logger.error(s"$code: $detail; reconciliation required, no automatic redispatch")
    progress(ctx, "RECOVERY_REQUIRED", s"$code: $detail", "PhaseWaitRecovery")
    Future.failed(RecoveryRequired(code, detail, cause))
  }

  /** A single area request. accepted=true/QUEUED is only queue admission, not
    * permission to dispatch. In particular, never retry StartProcess: it has no
    * operation identity and another send appends another queue entry. A lost
    * reply also cannot be interpreted as rejection or safely resent.
    * No-area direct invocation is retained for compatibility, NOT recovery proof.
    */
  private def askArea(ctx: FabDemoContext, areaType: String, expectedStatus: String,
                      mk: Option[ActorRef[EquipmentAreaActor.AreaReply]] => EquipmentAreaActor.Command): Future[Unit] =
    checked(ctx) {
      ctx.areaActorOf(areaType) match {
        case None => Future.successful(())
        case Some(ref) =>
          implicit val timeout: Timeout = 5.seconds
          checked(ctx)(ref.ask((reply: ActorRef[EquipmentAreaActor.AreaReply]) => mk(Some(reply))))
            .recoverWith {
              case ex if ex eq StaleRun => Future.failed(StaleRun)
              case NonFatal(ex) => recovery(ctx, "AREA_OUTCOME_UNKNOWN",
                s"$areaType command reply lost; area may already have changed: ${ex.getMessage}", ex)
            }(ctx.ec)
            .flatMap { reply =>
              requireFresh(ctx)
              if (reply.accepted && reply.status == expectedStatus) Future.successful(())
              else {
                val code = if (reply.accepted && reply.status == "QUEUED") "AREA_QUEUED_NOT_GRANTED"
                  else if (reply.accepted) "AREA_GRANT_UNKNOWN" else "AREA_NOT_GRANTED"
                recovery(ctx, code,
                  s"$areaType expected $expectedStatus, received accepted=${reply.accepted} status=${reply.status} reason=${reply.reason}")
              }
            }(ctx.ec)
      }
    }

  private def send(ctx: FabDemoContext, equipmentId: String, command: EquipmentCommand): Future[EquipmentEvent] =
    checked(ctx) {
      checked(ctx)(ctx.adapter.sendCommand(equipmentId, command)).recoverWith {
      case ex if ex eq StaleRun => Future.failed(StaleRun)
      case ex: ActorEquipmentAdapter.OutcomeUnknown =>
        requireFresh(ctx)
        progress(ctx, "RECOVERY_REQUIRED", s"EQUIPMENT_OUTCOME_UNKNOWN: ${ex.getMessage}", "PhaseWaitRecovery")
        Future.failed(ex) // keep the transport category/cause; never synthesize JobFailed
      case ex: ActorEquipmentAdapter.JobBusy =>
        requireFresh(ctx)
        progress(ctx, "RECOVERY_REQUIRED", s"EQUIPMENT_JOB_BUSY: ${ex.getMessage}", "PhaseWaitRecovery")
        Future.failed(ex)
      case ex: ActorEquipmentAdapter.SimulatorUnavailable =>
        requireFresh(ctx)
        progress(ctx, "RECOVERY_REQUIRED", s"EQUIPMENT_UNAVAILABLE: ${ex.getMessage}", "PhaseWaitRecovery")
        Future.failed(ex)
      case NonFatal(ex) => recovery(ctx, "EQUIPMENT_OUTCOME_UNKNOWN",
        s"$equipmentId command outcome is unknown: ${ex.getMessage}", ex)
      }(ctx.ec)
    }

  private def unexpected[A](ctx: FabDemoContext, equipmentId: String, response: EquipmentEvent): Future[A] =
    recovery(ctx, "EQUIPMENT_REPLY_UNKNOWN",
      s"$equipmentId returned an uncorrelated/unexpected response: $response")

  private def validResult(jobId: String, recipeId: String, result: EquipmentResult): Boolean =
    jobId.nonEmpty && (result match {
      case LithoExposureResult(resultJob, recipe, _) => resultJob == jobId && recipe == recipeId
      case MetrologyResult(resultJob, _) => resultJob == jobId
    })

  private def equipmentFailed(ctx: FabDemoContext, stage: String, equipmentId: String,
                              area: String, failure: JobFailed): Future[FabDemoState] = {
    requireFresh(ctx)
    val err = StageError(stage, Some(equipmentId), failure.errorCode, failure.detail)
    publish(ctx,PipelineStageFailed(err.stageName, err.equipId, err.errorCode, err.detail))
    publish(ctx,ProcessingCompleted(equipmentId, failure.jobId, success = false, failure.detail))
    requireFresh(ctx)
    ctx.areaActorOf(area).foreach { ref =>
      requireFresh(ctx)
      ref ! EquipmentAreaActor.ReportFault(equipmentId, failure.errorCode, failure.detail)
    }
    Future.failed(StageFailedException(err))
  }

  def loadFoup(state: FabDemoState, ctx: FabDemoContext): Future[FabDemoState] = inRun(ctx) {
    emitLedger(state, "PhaseLoad: Load FOUP from Stocker", ctx)
    progress(ctx, "LOADING", "Loading FOUP", "PhaseLoad")
    notifyLot(ctx)(RecordFoupLoaded(ctx.foupId, ctx.scenario.stocker.equipmentId, _))
    publish(ctx,FoupStateChanged(ctx.foupId, "LOADING", activeCount(state), 0, "STOCKER", lotId = ctx.scenario.scenarioId))
    publish(ctx,FoupArrivedAtPort(ctx.foupId, ctx.scenario.stocker.equipmentId, "STOCKER-PORT-1"))
    requireFresh(ctx)
    Future.successful(state.copy(ledgerSeq = state.ledgerSeq + 1, currentArea = "STOCKER"))
  }

  def transport(state: FabDemoState, ctx: FabDemoContext, from: String, to: String): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, s"PhaseTransport: $from → $to", ctx)
    progress(ctx, "TRANSPORTING", s"$from → $to", "PhaseTransport")
    val routeMs = ctx.scenario.amhs.routes.get((from, to)).map(_.toMillis).getOrElse(2000L)
    val scaledMs = (routeMs / ctx.speedMultiplier).toLong
    notifyLot(ctx)(RecordTransportStarted(ctx.foupId, from, to, scaledMs, _))
    publish(ctx,FoupInTransit(ctx.foupId, from, to, scaledMs / 2))
    publish(ctx,FoupStateChanged(ctx.foupId, "IN_TRANSIT", activeCount(state), 0, "AMHS", lotId = ctx.scenario.scenarioId))
    send(ctx, "AMHS", TransferFoup(ctx.foupId, from, to)).flatMap {
      case FoupArrived(foup, _) if foup == ctx.foupId =>
        requireFresh(ctx)
        Future.successful(s.copy(ledgerSeq = s.ledgerSeq + 1, currentArea = s"$from → $to"))
      case other => unexpected(ctx, "AMHS", other)
    }(ctx.ec)
  }

  def trackIn(state: FabDemoState, ctx: FabDemoContext, equipId: String, portId: String = "LP1"): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, s"PhaseTrackIn: $equipId port $portId", ctx)
    progress(ctx, "TRACK_IN", s"$equipId:$portId loading", "PhaseTrackIn")
    publish(ctx,FoupArrivedAtPort(ctx.foupId, equipId, portId))
    val areaType = if (equipId.contains("LITHO")) "LITHO" else if (equipId.contains("CDSEM")) "METROLOGY" else equipId
    publish(ctx,FoupStateChanged(ctx.foupId, "ON_PORT", activeCount(state), 0, equipId, lotId = ctx.scenario.scenarioId))
    askArea(ctx, areaType, EquipmentAreaActor.Loaded,
      reply => EquipmentAreaActor.TrackIn(equipId, s"lot-${s.ledgerSeq}", reply)).map { _ =>
      requireFresh(ctx)
      s.copy(ledgerSeq = s.ledgerSeq + 1, currentArea = equipId)
    }(ctx.ec)
  }

  def atEquipment(state: FabDemoState, ctx: FabDemoContext, area: String, equipId: String): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, s"PhaseAt$area: FOUP at $area", ctx)
    progress(ctx, "AT_EQP", s"FOUP at $area", s"PhaseAt$area")
    notifyLot(ctx)(RecordTransportCompleted(ctx.foupId, equipId, _))
    publish(ctx,FoupArrivedAtPort(ctx.foupId, equipId, s"$equipId-PORT-1"))
    publish(ctx,FoupStateChanged(ctx.foupId, "AT_EQUIPMENT", activeCount(state), 0, area, lotId = ctx.scenario.scenarioId))
    requireFresh(ctx)
    Future.successful(s.copy(ledgerSeq = s.ledgerSeq + 1, currentArea = area))
  }

  def process(state: FabDemoState, ctx: FabDemoContext, equipId: String, recipeId: String, areaType: String): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, s"PhaseProcess: $recipeId on $equipId", ctx)
    progress(ctx, "PROCESSING", s"$equipId processing", "PhaseProcess")
    val scaledMs = (ctx.scenario.litho.processingTime.toMillis / ctx.speedMultiplier).toLong
    askArea(ctx, areaType, EquipmentAreaActor.Busy,
      reply => EquipmentAreaActor.StartProcess(equipId, recipeId, scaledMs, reply)).flatMap { _ =>
      requireFresh(ctx)
      notifyLot(ctx)(RecordEquipmentJobStarted(equipId, recipeId, _))
      publish(ctx,ProcessingStarted(equipId, recipeId, scaledMs))
      send(ctx, equipId, ProcessRecipe(recipeId)).flatMap {
        case JobCompleted(jobId, equipmentId, result) if equipmentId == equipId && validResult(jobId, recipeId, result) =>
          requireFresh(ctx)
          notifyLot(ctx)(RecordEquipmentJobCompleted(equipId, jobId, success = true, _))
          publish(ctx,ProcessingCompleted(equipId, jobId, success = true, ""))
          askArea(ctx, areaType, EquipmentAreaActor.Finished,
            reply => EquipmentAreaActor.FinishProcess(equipId, reply)).map { _ =>
            requireFresh(ctx)
            s.copy(ledgerSeq = s.ledgerSeq + 1)
          }(ctx.ec)
        case failed: JobFailed if failed.equipmentId == equipId && failed.jobId.nonEmpty =>
          equipmentFailed(ctx, "Process", equipId, areaType, failed)
        case other => unexpected(ctx, equipId, other)
      }(ctx.ec)
    }(ctx.ec)
  }

  def trackOut(state: FabDemoState, ctx: FabDemoContext, equipId: String, portId: String = "LP1"): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, s"PhaseTrackOut: $equipId port $portId", ctx)
    progress(ctx, "TRACK_OUT", s"$equipId:$portId unloading", "PhaseTrackOut")
    val areaType = if (equipId.contains("LITHO")) "LITHO" else if (equipId.contains("CDSEM")) "METROLOGY" else equipId
    askArea(ctx, areaType, EquipmentAreaActor.Unloading,
      reply => EquipmentAreaActor.TrackOut(equipId, reply)).map { _ =>
      requireFresh(ctx)
      publish(ctx,FoupStateChanged(ctx.foupId, "UNLOADED", activeCount(state), 0, equipId, lotId = ctx.scenario.scenarioId))
      s.copy(ledgerSeq = s.ledgerSeq + 1)
    }(ctx.ec)
  }

  def measure(state: FabDemoState, ctx: FabDemoContext, equipId: String): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, "PhaseMeasure: CD-SEM", ctx)
    progress(ctx, "MEASURING", "CD measurement", "PhaseMeasure")
    val scaledMs = (ctx.scenario.cdSem.processingTime.toMillis / ctx.speedMultiplier).toLong
    askArea(ctx, "MET", EquipmentAreaActor.Busy,
      reply => EquipmentAreaActor.StartProcess(equipId, "CD-MEASURE-001", scaledMs, reply)).flatMap { _ =>
      requireFresh(ctx)
      notifyLot(ctx)(RecordEquipmentJobStarted(equipId, "CD-MEASURE-001", _))
      publish(ctx,ProcessingStarted(equipId, "CD-MEASURE-001", scaledMs))
      send(ctx, equipId, ProcessRecipe("CD-MEASURE-001")).flatMap {
        case JobCompleted(jobId, equipmentId, MetrologyResult(resultJob, measurements))
            if equipmentId == equipId && jobId.nonEmpty && resultJob == jobId &&
              s.wafers.keySet.subsetOf(measurements.keySet) &&
              measurements.forall { case (wid, cd) => wid == cd.waferId && java.lang.Double.isFinite(cd.measuredNm) } =>
          requireFresh(ctx)
          notifyLot(ctx)(RecordEquipmentJobCompleted(equipId, jobId, success = true, _))
          publish(ctx,ProcessingCompleted(equipId, jobId, success = true, ""))
          askArea(ctx, "MET", EquipmentAreaActor.Finished,
            reply => EquipmentAreaActor.FinishProcess(equipId, reply)).map { _ =>
            requireFresh(ctx)
            val cdValues = measurements.map { case (wid, cd) => wid -> cd.measuredNm }
            logger.info(s"[Measure] CDSEM returned ${cdValues.size} wafer CD values: $cdValues")
            val newWafers = s.wafers.map { case (wid, info) =>
              val cdVal = cdValues.get(wid)
              if (cdVal.isEmpty) logger.warn(s"[Measure] No CD value for wafer $wid in CDSEM response")
              wid -> info.copy(cdValueHistory = info.cdValueHistory ++ cdVal.toList)
            }
            s.copy(ledgerSeq = s.ledgerSeq + 1, wafers = newWafers)
          }(ctx.ec)
        case failed: JobFailed if failed.equipmentId == equipId && failed.jobId.nonEmpty =>
          equipmentFailed(ctx, "Measure", equipId, "MET", failed)
        case other => unexpected(ctx, equipId, other)
      }(ctx.ec)
    }(ctx.ec)
  }

  def classifyCd(cdValue: Double, config: DecisionConfig): String = {
    if (cdValue >= config.lowerSpecNm && cdValue <= config.upperSpecNm) "PASS"
    else if (cdValue > config.upperSpecNm && cdValue <= config.upperSpecNm + config.borderlineWindowNm) "BORDERLINE"
    else if (cdValue > config.upperSpecNm + 8.0) "SCRAP"
    else "FAIL"
  }

  def sealComplete(state: FabDemoState, ctx: FabDemoContext): Future[FabDemoState] = inRun(ctx) {
    val s = emitLedger(state, "PhaseComplete: Lot sealed", ctx)
    progress(ctx, "COMPLETED", "Demo completed", "PhaseComplete")
    notifyLot(ctx)(RecordTransportCompleted(ctx.foupId, ctx.scenario.stocker.equipmentId, _))
    notifyLot(ctx)(SealLot(_))
    val totalRework = state.wafers.values.count(_.reworkCount > 0)
    notifyLot(ctx)(CompleteProcess(ctx.scenario.scenarioId, state.passCount, state.scrapCount, totalRework, _))
    publish(ctx,FoupStateChanged(ctx.foupId, "COMPLETED", 0, 0, "STOCKER", lotId = ctx.scenario.scenarioId))
    publish(ctx,FoupArrivedAtPort(ctx.foupId, ctx.scenario.stocker.equipmentId, "STOCKER-PORT-1"))
    publish(ctx,LotUpdated(ctx.scenario.scenarioId, ctx.scenario.lotSize, state.scrapCount,
      (1 to state.iteration + 1).map(i => s"Completed-$i").toList, state.passCount, totalRework))
    publish(ctx,RecoveryCompleted(ctx.scenario.scenarioId, ctx.scenario.lotSize, state.passCount, totalRework, state.scrapCount))
    requireFresh(ctx)
    Future.successful(s.copy(ledgerSeq = s.ledgerSeq + 1, currentArea = "STOCKER"))
  }

  def activeCount(state: FabDemoState): Int =
    state.wafers.values.count(w => w.classification.isEmpty || w.classification.contains("FAIL"))

  def reworkCount(state: FabDemoState): Int =
    state.wafers.values.count(w => w.classification.contains("FAIL") && w.reworkCount > 0)

  def unresolvedIds(state: FabDemoState): Seq[String] =
    state.wafers.values.filter(w => w.classification.isEmpty || w.classification.contains("FAIL")).map(_.waferId).toSeq

  def cmdId(): String = UUID.randomUUID().toString.take(8)

  def scale(d: FiniteDuration, multiplier: Double): FiniteDuration =
    if (multiplier > 0) (d.toMillis / multiplier).toLong.millis else d

  def emitLedger(state: FabDemoState, name: String, ctx: FabDemoContext,
                 nodeId: Option[String] = None, subProcess: Option[String] = None,
                 branchDecision: Option[String] = None): FabDemoState = {
    requireFresh(ctx)
    publish(ctx,LedgerStepAdvanced(state.ledgerSeq, name, nodeId, subProcess, branchDecision))
    state
  }
}
