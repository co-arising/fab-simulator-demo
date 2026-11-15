package net.imadz.application.durable

import akka.util.ByteString
import net.imadz.monarch.persistence.batch.{BatchCodec, BatchDefinition, BatchSettings}
import net.imadz.monarch.persistence.operation.OperationProtocol.Request
import net.imadz.monarch.persistence.workflow._

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Base64
import scala.util.control.NonFatal

/** Business policy only. Each item is exactly ONE FOUP containing ONE wafer; no split/merge,
  * multi-wafer equipment batch, dynamic port selection, or consumer-owned execution state.
  */
object FabWorkflowModel {
  val Version = "complete-1"
  val LedgerEpoch = "ledger-1"
  val MaxItems = 10
  val MaxJobs = 16
  val MaxChildren = 2048
  val MaxResultBytes = 4096

  val definition: BatchDefinition = BatchDefinition("fab-durable-complete", Version, Vector(
    AreaDefinition("transport-in", Version, BatchPolicy.single, Map("amhs1" -> 1, "litho-port1" -> 1), maxExecutions = 1),
    AreaDefinition("litho", Version, BatchPolicy.single, Map("litho1" -> 1), maxExecutions = 1),
    AreaDefinition("transport-out", Version, BatchPolicy.single, Map("amhs1" -> 1, "metrology-port1" -> 1), maxExecutions = 1),
    AreaDefinition("measure", Version, BatchPolicy.single, Map("metrology1" -> 1), maxExecutions = 2),
    AreaDefinition("rework", Version, BatchPolicy.single, Map("rework-device1" -> 1), maxExecutions = 1),
    AreaDefinition("unload", Version, BatchPolicy.single, Map("amhs1" -> 1), maxExecutions = 1)
  ), BatchSettings(maxAcceptedItems = MaxItems, maxActiveItems = MaxItems, maxActiveBytes = 20480,
    maxItemBytes = 2048, maxResultBytes = MaxResultBytes, maxConcurrentBatches = 4, maxTransitions = 8,
    maxBatchCalls = 1200, maxReconciliations = 2, pollIntervalMillis = 250, callTimeoutMillis = 5000))

  val capacities: Map[String, Int] = definition.areas.flatMap(_.resources.keys).map(_ -> 1).toMap
  private val maxBatchesPerJob = MaxItems * definition.areas.map(_.maxExecutions).sum
  private val outcomes = Set("SUCCESS", "RETRY", "REWORK")
  private val jobPattern = "[A-Za-z0-9][A-Za-z0-9_-]{0,47}"
  private val operationPattern = ("(" + jobPattern + ")/batch-(0|[1-9][0-9]{0,3})/execute").r

  final case class ItemIdentity(jobId: String, sequence: Int) {
    def itemId: String = s"$jobId-item-$sequence"
    def foupId: String = itemId
    def waferId: String = itemId + "/wafer-0"
  }
  final case class Receipt(area: String, rows: Vector[(String, String)])

  private[durable] def check(ok: Boolean, reason: String): Either[String, Unit] =
    if (ok) Right(()) else Left(reason)

  private[durable] def safely[A](body: => Either[String, A]): Either[String, A] =
    try body catch { case NonFatal(_) => Left("malformed Fab identity, manifest or evidence") }

  private[durable] def strictUtf8(bytes: ByteString): Either[String, String] = safely {
    Right(StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes.toArray)).toString)
  }

  def validJobId(jobId: String): Boolean = jobId != null && jobId.matches(jobPattern)

  def itemIdentity(itemId: String): Either[String, ItemIdentity] = safely {
    val at = itemId.lastIndexOf("-item-")
    if (at <= 0) Left("itemId must identify this example's stable FOUP/wafer")
    else {
      val job = itemId.substring(0, at)
      val number = itemId.substring(at + 6)
      if (!validJobId(job) || !number.matches("[0-9]")) Left("invalid job/item identity")
      else Right(ItemIdentity(job, number.toInt))
    }
  }

  def operationJob(operationKey: String): Either[String, String] = operationKey match {
    case operationPattern(job, number) if number.toInt < maxBatchesPerJob => Right(job)
    case _ => Left("operation key is not a bounded Fab batch execution")
  }

  def encodeId(id: String): String = Base64.getUrlEncoder.withoutPadding()
    .encodeToString(id.getBytes(StandardCharsets.UTF_8))

  private def decodeId(encoded: String): Either[String, String] = safely {
    for {
      _ <- check(encoded.nonEmpty && encoded.length <= 256, "invalid encoded itemId length")
      id <- strictUtf8(ByteString(Base64.getUrlDecoder.decode(encoded)))
      _ <- check(id.nonEmpty && encodeId(id) == encoded, "itemId must use canonical unpadded base64url UTF-8")
      _ <- itemIdentity(id)
    } yield id
  }

  /** Count is frozen in every offer, so changing count under the same job conflicts even
    * if the earlier HTTP response was lost. There are no random IDs or in-memory receipts.
    */
  def input(jobId: String, count: Int, sequence: Int): Item = {
    require(validJobId(jobId) && count >= 1 && count <= MaxItems && sequence >= 0 && sequence < count,
      "jobId/count/sequence outside the finite Fab input contract")
    val id = ItemIdentity(jobId, sequence)
    val payload = s"FI2:$count:${encodeId(id.itemId)}:${encodeId(id.foupId)}:${encodeId(id.waferId)}"
    Item(sequence.toLong, id.itemId, ByteString(payload).toVector, id.foupId, "transport-in")
  }

  def occupancyId(jobId: String, itemId: String, metrology: Boolean): String =
    jobId + (if (metrology) "/occupancy/metrology/" else "/occupancy/") + itemId
  def occupancyOwner(jobId: String, itemId: String): String = jobId + "/foup/" + itemId

  private def inputCount(payload: Vector[Byte]): Either[String, Int] =
    strictUtf8(ByteString(payload.toArray)).flatMap { text =>
      text.split(":", -1) match {
        case Array("FI2", count, _, _, _) if count.matches("[1-9]|10") => Right(count.toInt)
        case _ => Left("invalid immutable one-FOUP/one-wafer input payload")
      }
    }

  def validateManifest(m: BatchManifest): Either[String, Unit] = safely {
    for {
      _ <- check(validJobId(m.jobId), "invalid jobId")
      job <- operationJob(m.operationKey)
      _ <- check(job == m.jobId && m.sealedAt >= 0, "batch/job identity mismatch")
      area <- definition.byId.get(m.area).toRight("unknown Fab area")
      _ <- check(m.policyVersion == area.policyVersion && m.items.size == 1,
        "every Fab execution requires the frozen single-FOUP policy")
      item = m.items.head
      id <- itemIdentity(item.itemId)
      count <- inputCount(item.payload)
      _ <- check(id.jobId == m.jobId && id.sequence.toLong == item.sequence && id.sequence < count &&
        item.compatibilityKey == id.foupId && input(m.jobId, count, id.sequence).payload == item.payload,
        "item, FOUP, wafer, input count or job identity differs from the immutable offer")
      _ <- check(item.area == m.area && item.execution >= 0 && item.execution < area.maxExecutions &&
        item.executions.forall { case (name, n) => definition.byId.get(name).exists(a => n >= 0 && n <= a.maxExecutions) } &&
        item.lineage.size <= definition.settings.maxTransitions, "area execution/transition budget invalid")
      _ <- check(if (m.area == "transport-in") item.carried.isEmpty else {
        val metrology = Set("measure", "rework", "unload")(m.area)
        item.carried.size == 1 && item.carried.forall { claim =>
          claim.reservationId == occupancyId(m.jobId, item.itemId, metrology) &&
            claim.ownerOperationKey == occupancyOwner(m.jobId, item.itemId) && claim.token.nonEmpty &&
            claim.units == Map((if (metrology) "metrology-port1" else "litho-port1") -> 1)
        }
      }, "incoming port claim does not identify this exact FOUP and fixed route")
    } yield ()
  }

  def operation(m: BatchManifest): Request = {
    validateManifest(m).fold(reason => throw new IllegalArgumentException(reason), identity)
    Request(m.operationKey, m.items.map(i => s"${i.itemId}:${m.area}:${i.execution}").mkString("effect:", ",", ""),
      "execute", "fab", Version, LedgerEpoch, BatchCodec.manifestToBytes(m), Some(m.reservationId))
  }

  def manifest(request: Request): Either[String, BatchManifest] = safely {
    val m = BatchCodec.manifestFromBytes(request.payload)
    for {
      _ <- validateManifest(m)
      _ <- check(request == operation(m), "request envelope differs from its frozen Fab manifest")
    } yield m
  }

  def businessPayload(m: BatchManifest): String = "WF2:" + m.area + ":" +
    m.items.map(i => encodeId(i.itemId) + "~" + i.execution).mkString(",")

  /** RETRY is a wire vocabulary member, but this Fab policy does not authorize a
    * business retry. Technical reconciliation always keeps the original Request.
    */
  def parseReceipt(result: ByteString): Either[String, Receipt] = safely {
    for {
      _ <- check(result.nonEmpty && result.size <= MaxResultBytes, "result byte limit exceeded")
      text <- strictUtf8(result)
      receipt <- text.split(":", -1) match {
        case Array("WR2", area, body) if definition.byId.contains(area) =>
          val rows = body.split(",", -1).toVector
          if (rows.isEmpty || rows.size > MaxItems) Left("invalid per-item result count")
          else rows.foldLeft[Either[String, Vector[(String, String)]]](Right(Vector.empty)) { (acc, row) =>
            for {
              prior <- acc
              pair <- row.split("~", -1) match {
                case Array(encoded, outcome) => for {
                  id <- decodeId(encoded)
                  _ <- check(outcomes(outcome), "unknown external outcome")
                  _ <- check(outcome == "SUCCESS" || (area == "measure" && outcome == "REWORK"),
                    "outcome is not authorized for this Fab area")
                } yield id -> outcome
                case _ => Left("malformed per-item result")
              }
            } yield prior :+ pair
          }.flatMap { parsed =>
            check(parsed.map(_._1).distinct.size == parsed.size, "duplicate result itemId").map(_ => Receipt(area, parsed))
          }
        case _ => Left("malformed WR2 result or unknown area")
      }
    } yield receipt
  }

  def results(m: BatchManifest, result: ByteString): Either[String, Map[String, String]] = for {
    _ <- validateManifest(m)
    receipt <- parseReceipt(result)
    _ <- check(receipt.area == m.area && receipt.rows.map(_._1).toSet == m.items.map(_.itemId).toSet &&
      receipt.rows.size == m.items.size, "result must cover the frozen area and every manifest item exactly once")
  } yield receipt.rows.toMap
}
