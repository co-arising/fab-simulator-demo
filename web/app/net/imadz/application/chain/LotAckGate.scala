package net.imadz.application.chain

import akka.actor.typed.{ActorRef, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.util.Timeout
import net.imadz.application.aggregates.LotProtocol.{LotCommand, LotConfirmation}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/** Tracks the confirmations for one stage. A received reply is not necessarily
  * an acceptance: the lot may explicitly reject the command. */
trait LotAckGate {
  def tell(lotRef: ActorRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit
  def tell(lotRef: EntityRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit

  /** Closes registration and waits for all registered requests to settle. */
  def seal(): Future[Unit]
}

object LotAckGate {
  final case class LotRejected(detail: String) extends RuntimeException(detail)

  /** Direct demonstration calls have no persistent stage boundary. */
  def fireAndForget(replyTo: ActorRef[LotConfirmation]): LotAckGate = new LotAckGate {
    override def tell(lotRef: ActorRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit =
      lotRef ! cmd(replyTo)
    override def tell(lotRef: EntityRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit =
      lotRef ! cmd(replyTo)
    override def seal(): Future[Unit] = Future.successful(())
  }

  def tracking(timeout: Timeout)(implicit ec: ExecutionContext, scheduler: Scheduler): LotAckGate =
    new LotAckGate {
      private var pending = 0
      private var sealedFlag = false
      private var firstFailure: Option[Throwable] = None
      private val done = Promise[Unit]()

      // Registration and sealing share this monitor. No request can be sent
      // after seal has observed an empty set of pending confirmations.
      private def track(request: => Future[LotConfirmation]): Unit = synchronized {
        require(!sealedFlag, "lot acknowledgement gate is already sealed")
        pending += 1
        try request.onComplete(settle)
        catch { case NonFatal(ex) => settle(Failure(ex)) }
      }

      private def settle(result: Try[LotConfirmation]): Unit = synchronized {
        val failure = result match {
          case Success(confirmation) => confirmation.error.map(error => LotRejected(s"${error.code}: ${error.message}"))
          case Failure(ex) => Some(ex)
        }
        failure.foreach { ex =>
          // A timeout/transport error leaves the outcome unknown. It must not
          // be hidden by an earlier explicit rejection from another request.
          if (firstFailure.isEmpty || (firstFailure.get.isInstanceOf[LotRejected] && !ex.isInstanceOf[LotRejected]))
            firstFailure = Some(ex)
        }
        pending -= 1
        checkDone()
      }

      private def checkDone(): Unit = {
        if (sealedFlag && pending == 0) firstFailure match {
          case Some(ex) => done.tryFailure(ex)
          case None => done.trySuccess(())
        }
      }

      override def tell(lotRef: ActorRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit =
        track(lotRef.ask[LotConfirmation](replyTo => cmd(replyTo))(timeout, scheduler))

      override def tell(lotRef: EntityRef[LotCommand], cmd: ActorRef[LotConfirmation] => LotCommand): Unit = {
        implicit val t: Timeout = timeout
        track(lotRef.ask[LotConfirmation](replyTo => cmd(replyTo)))
      }

      override def seal(): Future[Unit] = synchronized {
        sealedFlag = true
        checkDone()
        done.future
      }
    }
}
