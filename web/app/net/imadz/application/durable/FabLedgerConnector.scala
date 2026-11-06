package net.imadz.application.durable

import akka.util.ByteString
import net.imadz.monarch.persistence.operation.ExternalOperationPort
import net.imadz.monarch.persistence.operation.OperationProtocol._

import scala.concurrent.{ExecutionContext, Future}

/** Connection-correlated fixture receipts, not certificates from real AMHS/equipment.
  * QUERY must return the ledger's stored key/digest; SUBMIT intentionally omits both.
  */
final class FabLedgerConnector(area: String, ledger: FabLedgerClient)(implicit ec: ExecutionContext)
    extends ExternalOperationPort {
  import FabWorkflowModel._
  require(definition.byId.contains(area), "unknown Fab connector area")
  override val connectorId = "fab"
  override val connectorVersion: String = Version

  private def checked(request: Request) = manifest(request).flatMap { m =>
    check(m.area == area, "connector area differs from the frozen request").map(_ => m)
  }

  override def submit(request: Request): Future[Observation] = checked(request) match {
    case Left(reason) => Future.successful(Unknown(reason))
    case Right(m) => ledger.call("SUBMIT", Vector("fab", request.operationKey, request.fingerprint,
      request.ledgerEpoch, request.effectKey, businessPayload(m))).map(observe(request, _, query = false, previous = None))
  }

  override def query(request: Request, lastEvidence: Option[Evidence]): Future[Observation] = checked(request) match {
    case Left(reason) => Future.successful(Unknown(reason))
    case Right(_) => ledger.call("QUERY", Vector(request.operationKey))
      .map(response => observe(request, response, query = true, previous = lastEvidence))
  }

  private def taskIdValid(id: Option[String]): Boolean = id.exists(s =>
    s.length <= 64 && s.matches("task-[1-9][0-9]*"))

  override def validateEvidence(request: Request, observation: WithEvidence): Either[String, Unit] = safely {
    for {
      m <- checked(request)
      proof = observation.evidence
      _ <- check(proof.operationKey == request.operationKey && proof.requestDigest == request.fingerprint &&
        proof.ledgerEpoch == request.ledgerEpoch && taskIdValid(proof.taskId),
        "ledger receipt key/digest/epoch/task does not match this request")
      _ <- observation match {
        case _: Accepted | _: Running => check(proof.payload == ByteString("running"), "invalid running-task receipt")
        case Confirmed(Succeeded(result, evidence)) => for {
          _ <- check(evidence.payload == result, "terminal result differs from its evidence")
          _ <- results(m, result)
        } yield ()
        case _ => Left("only matching running or successful WR2 receipts are authorized")
      }
    } yield ()
  }

  // Deliberately inherit ExternalOperationPort.validateSafeToSubmit's fail-closed default.
  // NotFound, elapsed time, loss of a reply, and a changed ledger epoch never authorize resend.
  private[durable] def observe(request: Request, response: Vector[String], query: Boolean,
      previous: Option[Evidence]): Observation = {
    def evidence(task: String, epoch: String, payload: String, key: String, digest: String): Evidence =
      Evidence(key, digest, epoch, Some(task), ByteString(payload))
    val observed: Observation = response match {
      case Vector("RUNNING", task, epoch) if !query =>
        Accepted(evidence(task, epoch, "running", request.operationKey, request.fingerprint))
      case Vector("SUCCEEDED", task, epoch, result) if !query =>
        Confirmed(Succeeded(ByteString(result), evidence(task, epoch, result, request.operationKey, request.fingerprint)))
      case Vector("RUNNING", task, epoch, key, digest) if query =>
        Running(evidence(task, epoch, "running", key, digest))
      case Vector("SUCCEEDED", task, epoch, result, key, digest) if query =>
        Confirmed(Succeeded(ByteString(result), evidence(task, epoch, result, key, digest)))
      case Vector("NOT_FOUND", epoch) if query && epoch.length <= 64 && epoch.matches("ledger-[1-9][0-9]*") =>
        NotFound(s"external history unavailable at $epoch; this is not safe-to-submit evidence")
      case Vector("REJECTED", reason) => Unknown("external request rejected: " + reason.take(256))
      case _ => Unknown("malformed or unrecognized external response")
    }
    observed match {
      case proof: WithEvidence =>
        val validation = for {
          _ <- validateEvidence(request, proof)
          _ <- check(previous.forall(p => p.operationKey == request.operationKey && p.requestDigest == request.fingerprint &&
            p.ledgerEpoch == request.ledgerEpoch && taskIdValid(p.taskId) && p.taskId == proof.evidence.taskId),
            "query changed the previously recorded task identity")
        } yield ()
        validation.fold(Unknown.apply, _ => observed)
      case other => other
    }
  }
}
