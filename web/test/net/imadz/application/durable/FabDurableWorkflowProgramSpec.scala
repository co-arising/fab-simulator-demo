package net.imadz.application.durable

import akka.util.ByteString
import net.imadz.monarch.persistence.batch.BatchCodec
import net.imadz.monarch.persistence.operation.OperationProtocol._
import net.imadz.monarch.persistence.resource.ResourceProtocol
import net.imadz.monarch.persistence.workflow._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext

/** Pure policy/codec contracts: no actor system, sockets, database or external service is started. */
final class FabDurableWorkflowProgramSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  import FabWorkflowModel._
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val ledger = new FabLedgerClient(19112) // Workers are lazy; these tests never call the network.
  private val program = new FabDurableWorkflowProgram(ledger)
  private val job = "fab-contract"
  private val first = BatchManifest(job, s"$job/batch-0", "transport-in", Version,
    Vector(input(job, 2, 1)), sealedAt = 1L)

  override protected def afterAll(): Unit = try ledger.close() finally super.afterAll()
  private def right[A](result: Either[String, A]): A = result.fold(reason => fail(reason), identity)
  private def receipt(m: BatchManifest, outcome: String = "SUCCESS"): ByteString =
    ByteString(s"WR2:${m.area}:${encodeId(m.items.head.itemId)}~$outcome")
  private def succeeded(m: BatchManifest, result: ByteString): Succeeded = {
    val request = operation(m)
    Succeeded(result, Evidence(request.operationKey, request.fingerprint, LedgerEpoch, Some("task-1"), result))
  }
  private def completion(m: BatchManifest, outcome: String = "SUCCESS"): BatchCompletion =
    right(program.complete(m, succeeded(m, receipt(m, outcome))))
  private def grant(m: BatchManifest): ResourceProtocol.Grant =
    ResourceProtocol.Grant(m.reservationId, m.operationKey, "execution-token", definition.byId(m.area).resources)
  private def grant(claim: ResourceClaim): ResourceProtocol.Grant =
    ResourceProtocol.Grant(claim.reservationId, claim.ownerOperationKey, claim.token, claim.units)
  private def proof(c: BatchCompletion, owner: String): ResourceProtocol.Proof =
    ResourceProtocol.Proof(owner, ByteString(c.resource.proof.toArray))

  // Construct the next immutable test fixture from the declared contract, not an executing host.
  private def routed(m: BatchManifest, c: BatchCompletion, batch: Int): BatchManifest = {
    val route = c.decisions.head.disposition match {
      case value: ItemDisposition.Route => value
      case other => fail(s"expected a route, got $other")
    }
    val claims = if (c.retainCarried) m.items.head.carried else c.resource match {
      case ResourceDisposition.Carry(id, owner, units, _) => Vector(ResourceClaim(id, owner, "carried-token", units))
      case _: ResourceDisposition.Release => Vector.empty
    }
    val old = m.items.head
    val item = old.copy(area = route.area, compatibilityKey = route.compatibilityKey,
      executions = old.executions.updated(old.area, old.execution + 1), lineage = old.lineage :+ m.batchId,
      carried = claims)
    BatchManifest(job, s"$job/batch-$batch", route.area, Version, Vector(item), sealedAt = batch + 1L)
  }

  "Fab immutable input and manifest" should {
    "freeze count, contiguous offer sequence and job-prefixed FOUP/wafer identity" in {
      val item = input(job, 2, 1)
      item shouldBe input(job, 2, 1)
      item.sequence shouldBe 1L
      item.itemId shouldBe s"$job-item-1"
      item.compatibilityKey shouldBe item.itemId
      item.payload should not be input(job, 3, 1).payload
      itemIdentity(item.itemId) shouldBe Right(ItemIdentity(job, 1))
      ItemIdentity(job, 1).waferId shouldBe s"$job-item-1/wafer-0"
      Vector("", "../job", "job/slash", "a" * 49).foreach(id => validJobId(id) shouldBe false)
      Vector(0, 11).foreach(count => intercept[IllegalArgumentException](input(job, count, 0)))
      intercept[IllegalArgumentException](input(job, 2, 2))
    }

    "produce a stable public request and exact WF2 payload" in {
      val request = operation(first)
      request shouldBe operation(first)
      request.operationKey shouldBe s"$job/batch-0/execute"
      manifest(request) shouldBe Right(first)
      BatchCodec.manifestFromBytes(request.payload) shouldBe first
      businessPayload(first) shouldBe s"WF2:transport-in:${encodeId(first.items.head.itemId)}~0"
      manifest(request.copy(effectKey = "different-effect")).isLeft shouldBe true
      operationJob(s"$job/batch-70/execute").isLeft shouldBe true
      operationJob(s"$job/batch-00/execute").isLeft shouldBe true
    }

    "reject missing, duplicate, foreign and modified items or an absent port claim" in {
      val terminal = succeeded(first, receipt(first))
      val item = first.items.head
      val malformed = Vector(
        first.copy(items = Vector.empty), first.copy(items = Vector(item, item)),
        first.copy(jobId = "other-job"), first.copy(policyVersion = "other-version"),
        first.copy(items = Vector(item.copy(sequence = 0))),
        first.copy(items = Vector(item.copy(payload = ByteString("FI2:2:tampered").toVector))),
        first.copy(items = Vector(item.copy(compatibilityKey = "foreign-foup"))))
      malformed.foreach(m => program.complete(m, terminal).isLeft shouldBe true)
      val litho = routed(first, completion(first), 1)
      validateManifest(litho.copy(items = Vector(litho.items.head.copy(carried = Vector.empty)))).isLeft shouldBe true
      val claim = litho.items.head.carried.head
      validateManifest(litho.copy(items = Vector(litho.items.head.copy(
        carried = Vector(claim.copy(ownerOperationKey = "another/foup/another-item-1")))))).isLeft shouldBe true
    }
  }

  "Fab terminal receipts" should {
    "require every frozen item exactly once with the correct area and canonical codec" in {
      val id = encodeId(first.items.head.itemId)
      val invalid = Vector("", "WR2:transport-in:",
        s"WR2:transport-in:$id~SUCCESS,$id~SUCCESS",
        s"WR2:transport-in:${encodeId(s"$job-item-0")}~SUCCESS",
        s"WR2:litho:$id~SUCCESS", s"WR2:transport-in:$id~REWORK",
        s"WR2:transport-in:$id~RETRY", s"WR2:transport-in:$id~FAILED",
        s"WR2:transport-in:$id=~SUCCESS", s"WR2:transport-in:$id~SUCCESS:trailing")
      invalid.foreach(text => program.complete(first, succeeded(first, ByteString(text))).isLeft shouldBe true)
      parseReceipt(ByteString(Array(0xff.toByte))).isLeft shouldBe true
      parseReceipt(ByteString("x" * (MaxResultBytes + 1))).isLeft shouldBe true
    }

    "reject wrong key, digest, epoch, task identity and mismatched evidence payload" in {
      val terminal = succeeded(first, receipt(first))
      val evidence = terminal.evidence
      val invalid = Vector(evidence.copy(operationKey = "other/batch-0/execute"),
        evidence.copy(requestDigest = "wrong-digest"), evidence.copy(ledgerEpoch = "ledger-2"),
        evidence.copy(taskId = None), evidence.copy(taskId = Some("task-0")),
        evidence.copy(payload = ByteString("not-the-result")))
      invalid.foreach(e => program.complete(first, terminal.copy(evidence = e)).isLeft shouldBe true)
    }

    "never turn even a correctly correlated KnownFailed into success, a retry or resource release" in {
      val terminal = succeeded(first, receipt(first))
      val failed = KnownFailed("DEVICE_FAILED", "no authorized physical release receipt", terminal.result, terminal.evidence)
      // This fixture has no failure-release certificate. Failing closed is intentional:
      // returning Left cannot create an ItemDisposition.Retry or release a claimed port.
      program.complete(first, failed).isLeft shouldBe true
      program.connector(first.area).validateEvidence(operation(first), Confirmed(failed)).isLeft shouldBe true
    }

    "correlate QUERY using stored evidence and refuse NotFound or changed tasks as resend authority" in {
      val connector = new FabLedgerConnector(first.area, ledger)
      val request = operation(first)
      val previous = Evidence(request.operationKey, request.fingerprint, LedgerEpoch, Some("task-1"), ByteString("running"))
      val good = Vector("SUCCEEDED", "task-1", LedgerEpoch, receipt(first).utf8String,
        request.operationKey, request.fingerprint)
      connector.observe(request, good, query = true, previous = Some(previous)) shouldBe Confirmed(succeeded(first, receipt(first)))
      Vector(good.updated(1, "task-2"), good.updated(2, "ledger-2"),
        good.updated(4, "other/batch-0/execute"), good.updated(5, "wrong-digest"), good.take(4)).foreach { reply =>
        connector.observe(request, reply, query = true, previous = Some(previous)) shouldBe a[Unknown]
      }
      connector.observe(request, Vector("NOT_FOUND", "ledger-2"), query = true, previous = Some(previous)) shouldBe a[NotFound]
      connector.validateSafeToSubmit(request, previous, Some(previous)).isLeft shouldBe true
    }
  }

  "The complete Fab route" should {
    "retain exact occupied ports through processing and rework, then release all at unload" in {
      val imported = completion(first)
      imported.retainCarried shouldBe false
      imported.resource shouldBe a[ResourceDisposition.Carry]
      FabResourceEvidence.validate(grant(first), proof(imported, first.operationKey)) shouldBe Right(())
      val litho = routed(first, imported, 1)
      val lithoClaim = litho.items.head.carried.head
      lithoClaim.units shouldBe Map("litho-port1" -> 1)
      val processed = completion(litho)
      processed.retainCarried shouldBe true
      processed.resource shouldBe a[ResourceDisposition.Release] // Device only; incoming port retained.
      FabResourceEvidence.validate(grant(lithoClaim), proof(processed, lithoClaim.ownerOperationKey)).isLeft shouldBe true

      val transportOut = routed(litho, processed, 2)
      transportOut.items.head.carried shouldBe litho.items.head.carried
      val moved = completion(transportOut)
      moved.retainCarried shouldBe false
      moved.resource shouldBe a[ResourceDisposition.Carry]
      FabResourceEvidence.validate(grant(lithoClaim), proof(moved, lithoClaim.ownerOperationKey)) shouldBe Right(())
      val measure = routed(transportOut, moved, 3)
      val metrologyClaim = measure.items.head.carried.head
      metrologyClaim.units shouldBe Map("metrology-port1" -> 1)
      val reworkRequired = completion(measure, "REWORK")
      reworkRequired.retainCarried shouldBe true
      FabResourceEvidence.validate(grant(metrologyClaim), proof(reworkRequired, metrologyClaim.ownerOperationKey)).isLeft shouldBe true

      val rework = routed(measure, reworkRequired, 4)
      rework.area shouldBe "rework"
      rework.items.head.carried shouldBe measure.items.head.carried
      val reworked = completion(rework)
      reworked.retainCarried shouldBe true
      val remeasure = routed(rework, reworked, 5)
      remeasure.area shouldBe "measure"
      remeasure.items.head.execution shouldBe 1
      remeasure.items.head.carried shouldBe measure.items.head.carried
      program.complete(remeasure, succeeded(remeasure, receipt(remeasure, "REWORK"))).isLeft shouldBe true
      val accepted = completion(remeasure)
      accepted.retainCarried shouldBe true
      val unload = routed(remeasure, accepted, 6)
      unload.area shouldBe "unload"
      unload.items.head.carried shouldBe measure.items.head.carried
      val unloaded = completion(unload)
      unloaded.retainCarried shouldBe false
      unloaded.resource shouldBe a[ResourceDisposition.Release]
      unloaded.decisions.head.disposition shouldBe a[ItemDisposition.Completed]
      FabResourceEvidence.validate(grant(metrologyClaim), proof(unloaded, metrologyClaim.ownerOperationKey)) shouldBe Right(())
      FabResourceEvidence.validate(grant(unload), proof(unloaded, unload.operationKey)) shouldBe Right(())
      BatchCodec.completionFromBytes(BatchCodec.completionToBytes(accepted)) shouldBe accepted
      BatchCodec.completionFromBytes(BatchCodec.completionToBytes(unloaded)) shouldBe unloaded

      val foreign = grant(metrologyClaim).copy(ownerOperationKey = "foreign/foup/foreign-item-1")
      FabResourceEvidence.validate(foreign, proof(unloaded, foreign.ownerOperationKey)).isLeft shouldBe true
      FabResourceEvidence.validate(grant(unload), proof(unloaded, "wrong-owner")).isLeft shouldBe true
    }

    "route a successful first measurement directly to unload, without another business attempt" in {
      val litho = routed(first, completion(first), 1)
      val transportOut = routed(litho, completion(litho), 2)
      val measure = routed(transportOut, completion(transportOut), 3)
      val accepted = completion(measure)
      accepted.decisions.head.disposition shouldBe ItemDisposition.Route("unload", measure.items.head.compatibilityKey)
      accepted.retainCarried shouldBe true
      accepted.decisions.exists(_.disposition.isInstanceOf[ItemDisposition.Retry]) shouldBe false
    }
  }

  "The external wire codec" should {
    "round-trip strict UTF-8 and reject invalid, oversized and truncated frames without I/O" in {
      val fields = Vector("TASKS", "晶圆", "job/batch-0/execute")
      FabLedgerWire.read(new ByteArrayInputStream(FabLedgerWire.frame(fields))) shouldBe fields
      intercept[IOException](FabLedgerWire.frame(Vector.fill(17)("x")))
      intercept[IOException](FabLedgerWire.frame(Vector("x" * (FabLedgerWire.MaxStringBytes + 1))))
      val invalidLength = ByteBuffer.allocate(12).putInt(8).putInt(1).putInt(-1).array()
      intercept[IOException](FabLedgerWire.read(new ByteArrayInputStream(invalidLength)))
      val frame = FabLedgerWire.frame(Vector("x"))
      intercept[IOException](FabLedgerWire.read(new ByteArrayInputStream(frame.dropRight(1))))
      frame(frame.length - 1) = 0xff.toByte
      intercept[IOException](FabLedgerWire.read(new ByteArrayInputStream(frame)))
    }
  }
}
