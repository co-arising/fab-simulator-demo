package net.imadz.fab.protocol

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object ActorEquipmentAdapter {
  /** Admission rejected for this request; the reported job is NOT a failed job. */
  final case class JobBusy(equipmentId: String, status: StatusReport)
    extends RuntimeException(s"Equipment '$equipmentId' is busy; recipe admission rejected")

  /**
   * No reliable command outcome/status was received. This anonymous protocol has
   * no stable operation identity: querying current status is possible, but even
   * Idle cannot prove an earlier command never executed. Do not blindly replay
   * during recovery or translate this failure into a hardware JobFailed.
   */
  final case class OutcomeUnknown(equipmentId: String, command: EquipmentCommand, cause: Throwable)
    extends RuntimeException(s"Outcome unknown for '$equipmentId' command $command", cause)

  /** No command was sent by this call; this says nothing about earlier execution. */
  final case class SimulatorUnavailable(equipmentId: String)
    extends RuntimeException(s"No simulator registered for equipment '$equipmentId'")
}

/**
 * Akka Typed Actor-based equipment adapter — in-process simulation transport.
 *
 * Each equipment simulator is an Akka Typed Actor. This adapter looks up
 * (or is injected with) actor references and communicates via ask pattern.
 *
 * This is the default transport for the Fab simulation demo.
 * For real equipment, replace with GrpcEquipmentAdapter.
 */
class ActorEquipmentAdapter(askTimeout: FiniteDuration = 5.minutes)(implicit
  system: ActorSystem[Nothing],
  ec: ExecutionContext
) extends EquipmentAdapter[Future] {

  import ActorEquipmentAdapter._

  override def adapterId: String = "akka-actor-inprocess"

  private implicit val timeout: Timeout = Timeout(askTimeout)

  // Immutable snapshots; every registry/subscriber access is guarded by this lock.
  private val stateLock = new Object
  private var simulators: Map[String, ActorRef[SimulatorCommand]] = Map.empty
  private var subscribers: Map[String, List[EquipmentEvent => Unit]] = Map.empty

  /** Register a simulator actor for a given equipment ID during bootstrap. */
  def registerSimulator(equipmentId: String, ref: ActorRef[SimulatorCommand]): Unit = stateLock.synchronized {
    simulators = simulators + (equipmentId -> ref)
  }

  private def simulatorFor(equipmentId: String): Option[ActorRef[SimulatorCommand]] =
    stateLock.synchronized { simulators.get(equipmentId) }

  private def askCommand(equipmentId: String, cmd: EquipmentCommand): Future[EquipmentEvent] = {
    simulatorFor(equipmentId) match {
      case Some(ref) =>
        // Sending and all future/callback execution happen outside the registry lock.
        try {
          ref.ask[EquipmentEvent](replyTo => SimulateCommand(cmd, replyTo))
            .recoverWith { case NonFatal(cause) =>
              Future.failed(OutcomeUnknown(equipmentId, cmd, cause))
            }
        } catch {
          case NonFatal(cause) => Future.failed(OutcomeUnknown(equipmentId, cmd, cause))
        }
      case None => Future.failed(SimulatorUnavailable(equipmentId))
    }
  }

  override def sendCommand(equipmentId: String, cmd: EquipmentCommand): Future[EquipmentEvent] = {
    askCommand(equipmentId, cmd).map { reply =>
      (cmd, reply) match {
        case (_: ProcessRecipe, status: StatusReport) if status.equipmentId == equipmentId && status.status == Busy =>
          throw JobBusy(equipmentId, status)
        case (_: ProcessRecipe, completed: JobCompleted) if completed.equipmentId == equipmentId => completed
        case (_: ProcessRecipe, failed: JobFailed) if failed.equipmentId == equipmentId => failed
        case (_: ProcessRecipe, other) =>
          throw OutcomeUnknown(equipmentId, cmd,
            new IllegalStateException(s"Unexpected recipe reply: $other"))
        case (_, other) => other
      }
    }
  }

  override def queryStatus(equipmentId: String): Future[StatusReport] = {
    val cmd = QueryStatus()
    askCommand(equipmentId, cmd).map {
      case status: StatusReport if status.equipmentId == equipmentId => status
      case other =>
        throw OutcomeUnknown(equipmentId, cmd,
          new IllegalStateException(s"Unexpected status reply: $other"))
    }
  }

  override def subscribe(equipmentId: String)(callback: EquipmentEvent => Unit): Unit = stateLock.synchronized {
    val current = subscribers.getOrElse(equipmentId, Nil)
    subscribers = subscribers + (equipmentId -> (callback :: current))
  }

  override def unsubscribe(equipmentId: String): Unit = stateLock.synchronized {
    subscribers = subscribers - equipmentId
  }

  /** Inject a fault into a simulator (M3.5). Fire-and-forget — no return value. */
  def injectFault(equipmentId: String, faultType: String, duration: Option[FiniteDuration] = None): Unit = {
    simulatorFor(equipmentId).foreach { ref =>
      ref ! InjectFault(faultType, duration)
    }
  }

  /**
   * Deliver one immutable callback snapshot outside the lock. Unsubscribe affects
   * subsequent snapshots, not an in-flight notification; one failure cannot
   * prevent the other subscribers from receiving this event.
   */
  private[protocol] def notifySubscribers(equipmentId: String, event: EquipmentEvent): Unit = {
    val callbacks = stateLock.synchronized { subscribers.getOrElse(equipmentId, Nil) }
    callbacks.foreach { callback =>
      try callback(event)
      catch { case NonFatal(cause) => system.log.warn(s"Equipment subscriber failed for '$equipmentId'", cause) }
    }
  }
}

/**
 * Marker trait for commands accepted by equipment simulator actors.
 * NOT sealed — simulators define their own internal messages (e.g. timer ticks).
 */
trait SimulatorCommand

/** Wrapper command sent to simulator actors. */
case class SimulateCommand(cmd: EquipmentCommand, replyTo: ActorRef[EquipmentEvent]) extends SimulatorCommand

/** Internal timer tick — used by simulators with Akka Behaviors.withTimers */
case object TimerTick extends SimulatorCommand

/** Inject a fault into a simulator (M3.5). Sent via ActorEquipmentAdapter.injectFault(). */
case class InjectFault(faultType: String, duration: Option[FiniteDuration] = None) extends SimulatorCommand
