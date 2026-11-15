package net.imadz.application.chain

import akka.actor.typed.ActorSystem
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.{CurrentEventsByPersistenceIdQuery, ReadJournal}
import akka.stream.Materializer
import net.imadz.monarch.persistence.PersistentMonarch

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** A read-only view of committed execution facts. Observer messages are not
  * history; recovery validates this view before any interpreter is started. */
object FabRunHistory {
  final case class Checkpoint(cursor: String, state: FabRunState, error: Option[FabExecutionModel.StageError])
  sealed trait Terminal
  case object Completed extends Terminal
  final case class Failed(cursor: String, reason: String) extends Terminal
  final case class History(runId: String, initial: FabRunState, plan: Vector[String],
                           checkpoints: Vector[Checkpoint], terminal: Option[Terminal]) {
    def latest: FabRunState = checkpoints.lastOption.map(_.state).getOrElse(initial)
  }

  def read(system: ActorSystem[_], persistenceId: String)
          (implicit ec: ExecutionContext, mat: Materializer): Future[Option[History]] = {
    val config = system.settings.config
    val plugin = config.getString("fab.persistence.read-journal-plugin-id")
    val journal = PersistenceQuery(system).readJournalFor[ReadJournal with CurrentEventsByPersistenceIdQuery](plugin)
    journal.currentEventsByPersistenceId(persistenceId, 1L, Long.MaxValue)
      .runFold((0L, Option.empty[History])) { case ((sequence, history), envelope) =>
        require(envelope.sequenceNr == sequence + 1, s"run history sequence gap after $sequence")
        val event = envelope.event match {
          case e: PersistentMonarch.Event => e
          case other => throw new IllegalArgumentException(s"unsupported run event: ${other.getClass.getName}")
        }
        val next = append(history, event).fold(error => throw new IllegalArgumentException(error), identity)
        (envelope.sequenceNr, next)
      }.map(_._2)
  }

  def append(history: Option[History], event: PersistentMonarch.Event): Either[String, Option[History]] =
    try {
      val next = (history, event) match {
        case (None, PersistentMonarch.RunStarted(id, plan, bytes)) =>
          require(plan.distinct.size == plan.size, "duplicate stage cursors")
          val initial = FabRunStateCodec.protobuf.decode(bytes)
          require(initial.workOrderId == id && initial.completedCount == 0, "invalid initial run identity/frontier")
          History(id, initial, plan, Vector.empty, None)
        case (Some(h), PersistentMonarch.StageDone(cursor, _, _, bytes)) =>
          appendCheckpoint(h, cursor, bytes, None)
        case (Some(h), PersistentMonarch.StageResolved(cursor, _, code, errorCode, detail, bytes)) =>
          appendCheckpoint(h, cursor, bytes, Some(FabExecutionModel.StageError(cursor, code, errorCode, detail)))
        case (Some(h), PersistentMonarch.RunPlanExtended(plan)) =>
          require(h.terminal.isEmpty && plan.startsWith(h.plan) && plan.distinct.size == plan.size,
            "invalid plan extension")
          h.copy(plan = plan)
        case (Some(h), PersistentMonarch.RunCompleted(id)) =>
          require(h.terminal.isEmpty && id == h.runId && h.checkpoints.size == h.plan.size,
            "invalid completion frontier")
          h.copy(terminal = Some(Completed))
        case (Some(h), PersistentMonarch.RunFailedEvt(cursor, reason)) =>
          require(h.terminal.isEmpty, "duplicate terminal event")
          h.copy(terminal = Some(Failed(cursor, reason)))
        case _ => throw new IllegalArgumentException("unexpected event order in run history")
      }
      Right(Some(next))
    } catch { case NonFatal(ex) => Left(Option(ex.getMessage).getOrElse(ex.toString)) }

  private def appendCheckpoint(h: History, cursor: String, bytes: Array[Byte],
                               error: Option[FabExecutionModel.StageError]): History = {
    require(h.terminal.isEmpty && h.plan.lift(h.checkpoints.size).contains(cursor), "checkpoint is not the next plan cursor")
    val state = FabRunStateCodec.protobuf.decode(bytes)
    require(state.workOrderId == h.runId && state.scenarioId == h.initial.scenarioId &&
      state.completedCount == h.checkpoints.size + 1, "checkpoint identity/frontier mismatch")
    val outcome = error.orElse(state.lastOutcome.collect { case FabRunState.OcapResolved(err) => err })
    h.copy(checkpoints = h.checkpoints :+ Checkpoint(cursor, state, outcome))
  }
}
