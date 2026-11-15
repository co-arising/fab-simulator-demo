package net.imadz.application.durable

import akka.util.ByteString
import net.imadz.monarch.persistence.batch.{BatchDefinition, WorkflowProgram}
import net.imadz.monarch.persistence.operation.{ExternalOperationPort, OperationProtocol}
import net.imadz.monarch.persistence.resource.{ResourceEvidenceValidator, ResourceProtocol}
import net.imadz.monarch.persistence.workflow._

import scala.concurrent.ExecutionContext

/** The public WorkflowHost owns jobs, resource admission, inbox delivery, managed operations
  * and recovery. This class supplies only immutable policy and terminal-result classification.
  */
final class FabDurableWorkflowProgram(ledger: FabLedgerClient)(implicit ec: ExecutionContext) extends WorkflowProgram {
  override val definition: BatchDefinition = FabWorkflowModel.definition
  override val capacities: Map[String, Int] = FabWorkflowModel.capacities
  override val resourceEvidence: ResourceEvidenceValidator = FabResourceEvidence

  override def connector(area: String): ExternalOperationPort = new FabLedgerConnector(area, ledger)
  override def operation(manifest: BatchManifest): OperationProtocol.Request = FabWorkflowModel.operation(manifest)

  override def complete(m: BatchManifest, terminal: OperationProtocol.Terminal): Either[String, BatchCompletion] = {
    import FabWorkflowModel.{check, occupancyId, occupancyOwner, results, safely, validateManifest}
    safely {
      for {
        _ <- validateManifest(m)
        _ <- connector(m.area).validateEvidence(operation(m), OperationProtocol.Confirmed(terminal))
        confirmed <- results(m, terminal.result)
        item = m.items.head // The manifest validator enforces ONE FOUP / ONE wafer.
        _ <- check(!(m.area == "measure" && confirmed(item.itemId) == "REWORK" && item.execution != 0),
          "remeasurement cannot request another rework cycle")
        disposition <- (m.area, confirmed(item.itemId)) match {
          case ("transport-in", "SUCCESS") => Right(ItemDisposition.Route("litho", item.compatibilityKey))
          case ("litho", "SUCCESS") => Right(ItemDisposition.Route("transport-out", item.compatibilityKey))
          case ("transport-out", "SUCCESS") => Right(ItemDisposition.Route("measure", item.compatibilityKey))
          case ("measure", "SUCCESS") => Right(ItemDisposition.Route("unload", item.compatibilityKey))
          case ("measure", "REWORK") => Right(ItemDisposition.Route("rework", item.compatibilityKey))
          case ("rework", "SUCCESS") => Right(ItemDisposition.Route("measure", item.compatibilityKey))
          case ("unload", "SUCCESS") => Right(ItemDisposition.Completed(terminal.result.toVector))
          case _ => Left("no Fab business disposition is authorized by this terminal result")
        }
        // The first line binds the WR2 receipt to the operation that produced it. The
        // outer ResourceProtocol.Proof key is the owner of whichever claim is released.
        proof = (ByteString(m.operationKey + "\n") ++ terminal.result).toVector
        resource: ResourceDisposition = m.area match {
          case "transport-in" => ResourceDisposition.Carry(occupancyId(m.jobId, item.itemId, metrology = false),
            occupancyOwner(m.jobId, item.itemId), Map("litho-port1" -> 1), proof)
          case "transport-out" => ResourceDisposition.Carry(occupancyId(m.jobId, item.itemId, metrology = true),
            occupancyOwner(m.jobId, item.itemId), Map("metrology-port1" -> 1), proof)
          case _ => ResourceDisposition.Release(proof)
        }
        completion = BatchCompletion(Vector(ItemDecision(item.itemId, disposition)), resource,
          retainCarried = Set("litho", "measure", "rework")(m.area))
        _ <- BatchConservation.validate(m, completion, definition.byId, definition.settings.maxTransitions)
          .map(Left(_)).getOrElse(Right(()))
      } yield completion
    }
  }
}

/** Validation of the trusted local fixture's authoritative terminal receipt. Not a physical
  * sensor certificate: fixed route + immutable item identity are the domain boundary here.
  * There is no I/O, ledger copy, mutable allowlist, or string-prefix-only approval.
  */
private[durable] object FabResourceEvidence extends ResourceEvidenceValidator {
  import FabWorkflowModel._

  override def validate(grant: ResourceProtocol.Grant, proof: ResourceProtocol.Proof): Either[String, Unit] = safely {
    for {
      _ <- check(proof.operationKey == grant.ownerOperationKey, "resource proof key differs from grant owner")
      text <- strictUtf8(proof.evidence)
      parts = text.split("\n", -1)
      _ <- check(parts.length == 2, "resource proof requires operationKey, newline, and exact WR2 receipt")
      resultOperation = parts(0)
      job <- operationJob(resultOperation)
      receipt <- parseReceipt(ByteString(parts(1)))
      _ <- check(receipt.rows.size == 1, "resource evidence must identify exactly one FOUP")
      item <- itemIdentity(receipt.rows.head._1)
      _ <- check(item.jobId == job, "result item belongs to a different operation/job")
      _ <- operationJob(grant.ownerOperationKey) match {
        case Right(ownerJob) => check(ownerJob == job && resultOperation == grant.ownerOperationKey &&
          grant.reservationId == resultOperation.stripSuffix("/execute") + "/resources" &&
          grant.units == definition.byId(receipt.area).resources,
          "terminal operation/area does not authorize release or transfer of this execution grant")
        case Left(_) => validateOccupancy(grant, resultOperation, receipt, item)
      }
    } yield ()
  }

  private def validateOccupancy(grant: ResourceProtocol.Grant, resultOperation: String,
      receipt: Receipt, item: ItemIdentity): Either[String, Unit] = {
    grant.ownerOperationKey.split("/", -1) match {
      case Array(job, "foup", ownedItemId) => for {
        owner <- itemIdentity(ownedItemId)
        operationOwner <- operationJob(resultOperation)
        _ <- check(validJobId(job) && owner.jobId == job && operationOwner == job && owner == item &&
          receipt.rows == Vector(ownedItemId -> "SUCCESS"), "receipt does not authorize moving this exact owned FOUP")
        lithoRelease = grant.reservationId == occupancyId(job, ownedItemId, metrology = false) &&
          grant.units == Map("litho-port1" -> 1) && receipt.area == "transport-out"
        metrologyRelease = grant.reservationId == occupancyId(job, ownedItemId, metrology = true) &&
          grant.units == Map("metrology-port1" -> 1) && receipt.area == "unload"
        _ <- check(lithoRelease || metrologyRelease, "only the confirmed fixed-route move/unload releases this occupied port")
      } yield ()
      case _ => Left("unknown carried-occupancy owner identity")
    }
  }
}
