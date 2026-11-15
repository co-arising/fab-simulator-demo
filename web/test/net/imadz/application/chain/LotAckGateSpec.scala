package net.imadz.application.chain

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import net.imadz.application.aggregates.LotProtocol.{LotCommand, LotConfirmation, RecordWaferMeasured}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import net.imadz.common.CommonTypes.iMadzError
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.concurrent.duration._

/**
 * Proves the 2026-10-18 arbitration: lot notifications are acknowledged.
 *
 * Before: the pipeline told the lot "record this" via fire-and-forget
 * (`ignoreLotReply`) and moved on — a crashed or slow lot left the two sides
 * silently diverged.
 *
 * After: each step installs a tracking [[LotAckGate]]; the step only counts as
 * done once every lot notification is confirmed. An unconfirmed notification
 * fails the step as `LOT_ACK_TIMEOUT`, which the pipeline observes through the
 * normal step-failure path (OCAP evaluation / run failure).
 */
class LotAckGateSpec extends AnyWordSpec with Matchers with ScalaFutures {

  implicit private val patience: PatienceConfig = PatienceConfig(10.seconds)

  /** A lot actor that confirms every measurement immediately. */
  private def confirmingLot: Behavior[LotCommand] =
    Behaviors.receiveMessage {
      case RecordWaferMeasured(_, _, replyTo) =>
        replyTo ! LotConfirmation(None)
        Behaviors.same
      case _ => Behaviors.same
    }

  private def withSystem[T](name: String)(body: ActorSystem[Nothing] => T): T = {
    // Do not inherit the demo's cluster provider, seed nodes, or fixed port.
    val config = ConfigFactory.parseString("""
      akka.actor.provider = local
      akka.remote.artery.canonical.port = 0
      akka.cluster.seed-nodes = []
    """).withFallback(ConfigFactory.load())
    implicit val system: ActorSystem[Nothing] = ActorSystem[Nothing](Behaviors.empty, name, config)
    try body(system)
    finally {
      system.terminate()
      system.whenTerminated.futureValue
    }
  }

  "LotAckGate.tracking" should {

    "complete the seal when every notification is confirmed" in withSystem("gate-confirm") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      implicit val timeout: Timeout = 3.seconds

      val gate = LotAckGate.tracking(timeout)
      val lot = system.systemActorOf(confirmingLot, "confirming-lot")
      gate.tell(lot, RecordWaferMeasured(UUID.randomUUID(), 42.0, _))
      gate.tell(lot, RecordWaferMeasured(UUID.randomUUID(), 43.0, _))

      gate.seal().futureValue shouldBe (())
    }

    "fail the seal when the lot never confirms" in withSystem("gate-timeout") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      implicit val timeout: Timeout = 300.millis

      val gate = LotAckGate.tracking(timeout)
      // system.ignoreRef is a black hole: the ask can never complete.
      gate.tell(system.ignoreRef, RecordWaferMeasured(UUID.randomUUID(), 42.0, _))

      val failure = gate.seal().failed.futureValue
      failure shouldBe a[Exception]
    }

    "reject an explicit lot error for actor and sharded references" in withSystem("gate-rejected") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      val lot = system.systemActorOf(Behaviors.receiveMessage[LotCommand] {
        case RecordWaferMeasured(_, _, replyTo) =>
          replyTo ! LotConfirmation(Some(iMadzError("LOT_HELD", "review required")))
          Behaviors.same
        case _ => Behaviors.same
      }, "rejecting-lot")
      val actorGate = LotAckGate.tracking(Timeout(3.seconds))
      actorGate.tell(lot, RecordWaferMeasured(UUID.randomUUID(), 1.0, _))
      actorGate.seal().failed.futureValue shouldBe LotAckGate.LotRejected("LOT_HELD: review required")

      val shardedGate = LotAckGate.tracking(Timeout(3.seconds))
      val ref = TestEntityRef(EntityTypeKey[LotCommand]("test-lot"), "held", lot)
      shardedGate.tell(ref, RecordWaferMeasured(UUID.randomUUID(), 1.0, _))
      shardedGate.seal().failed.futureValue shouldBe LotAckGate.LotRejected("LOT_HELD: review required")
    }

    "refuse registration after sealing without sending a command" in withSystem("gate-sealed") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      val gate = LotAckGate.tracking(Timeout(3.seconds))
      gate.seal().futureValue shouldBe (())
      intercept[IllegalArgumentException] {
        gate.tell(system.ignoreRef, _ => throw new AssertionError("must not create a command after seal"))
      }
    }

    "settle a synchronous command construction failure" in withSystem("gate-sync-error") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      val gate = LotAckGate.tracking(Timeout(3.seconds))
      val failure = new IllegalStateException("cannot construct command")
      val lot = system.systemActorOf(confirmingLot, "lot")
      gate.tell(lot, _ => throw failure)
      gate.seal().failed.futureValue shouldBe failure
    }

    "wait for every registered confirmation even after one rejects" in withSystem("gate-drain") { implicit system =>
      implicit val ec = system.executionContext
      implicit val scheduler = system.scheduler
      val pendingReply = scala.concurrent.Promise[akka.actor.typed.ActorRef[LotConfirmation]]()
      val lot = system.systemActorOf(Behaviors.receiveMessage[LotCommand] {
        case RecordWaferMeasured(_, value, replyTo) if value == 1.0 =>
          replyTo ! LotConfirmation(Some(iMadzError("LOT_HELD", "review required")))
          Behaviors.same
        case RecordWaferMeasured(_, _, replyTo) =>
          pendingReply.success(replyTo)
          Behaviors.same
        case _ => Behaviors.same
      }, "lot")
      val gate = LotAckGate.tracking(Timeout(3.seconds))
      gate.tell(lot, RecordWaferMeasured(UUID.randomUUID(), 1.0, _))
      gate.tell(lot, RecordWaferMeasured(UUID.randomUUID(), 2.0, _))
      val result = gate.seal()
      val reply = pendingReply.future.futureValue
      result.isCompleted shouldBe false
      reply ! LotConfirmation(None)
      result.failed.futureValue shouldBe LotAckGate.LotRejected("LOT_HELD: review required")
    }
  }

  "LotAckGate.fireAndForget" should {

    "preserve the old behavior: seal always succeeds without tracking" in withSystem("gate-ff") { implicit system =>
      val gate = LotAckGate.fireAndForget(system.ignoreRef)
      gate.tell(system.ignoreRef, RecordWaferMeasured(UUID.randomUUID(), 42.0, _))

      gate.seal().futureValue shouldBe (())
    }
  }
}
