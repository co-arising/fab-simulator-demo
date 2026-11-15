package net.imadz.application.chain

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import net.imadz.application.aggregates.LotProtocol._
import net.imadz.application.chain.FabExecutionModel._
import net.imadz.application.chain.FabPipelineExecutionActor._
import net.imadz.application.scenario.StandardScenarios
import net.imadz.domain.events.FabSimulationEvent
import net.imadz.fab.protocol._
import net.imadz.monarch.persistence.PersistentMonarch
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration._

/** Real-host regression for admission and equipment result uncertainty. */
class EquipmentRecoveryBoundaryGapSpec
    extends ScalaTestWithActorTestKit(FabPipelineExecutionActorSpec.config) with AnyWordSpecLike {

  "The host equipment recovery boundary" should {
    "persist controlled recovery for equipment unknown without OCAP or a successful checkpoint" in {
      val id = UUID.randomUUID().toString
      val wafer = "W1"
      val scenario = StandardScenarios.photoCell5Wafer.copy(scenarioId = s"gap-$id", waferIds = Seq(wafer), lotSize = 1)
      val commands = createTestProbe[LotCommand]()
      val equipment = createTestProbe[SimulatorCommand]()
      val events = createTestProbe[FabSimulationEvent]()
      val lot = spawn(Behaviors.receiveMessage[LotCommand] { cmd =>
        commands.ref ! cmd
        cmd match {
          case RecordEquipmentJobStarted(_, _, reply) => reply ! LotConfirmation(None)
          case FailLot(_, _, reply) => reply ! LotConfirmation(None)
          case other => throw new IllegalArgumentException(s"Unexpected gap fixture Lot command: $other")
        }
        Behaviors.same
      })
      val lotRef = TestEntityRef(EntityTypeKey[LotCommand]("equipment-gap-lot"), id, lot)
      val adapter = new ActorEquipmentAdapter()(system, system.executionContext)
      adapter.registerSimulator("CDSEM-01", equipment.ref)
      def context(): FabDemoContext = FabDemoContext(scenario, s"FOUP-$id", lotRef, lotRef,
        Map(wafer -> UUID.randomUUID()), UUID.fromString(id), UUID.randomUUID(), adapter,
        events.ref ! _, system.ignoreRef[LotConfirmation],
        (_, _, _, _, _) => Future.failed(new IllegalStateException("No Saga in gap fixture")),
        speedMultiplier = 100.0)(system.executionContext)
      val stages = Seq(FabScenarioPipeline.Measure("CDSEM-01"))
      val parent = spawn(FabPipelineExecutionActor(id, (_, _) => context(),
        _ => throw new IllegalStateException("No fallback state"), _ => stages, events.ref ! _)(system.executionContext))
      val replies = createTestProbe[ExecutionReply]()
      parent ! StartExecution(scenario.scenarioId, id, FabDemoState(Map(wafer -> WaferInfo(wafer))), stages, context(), replies.ref)
      replies.expectMessage(Accepted)
      commands.expectMessageType[RecordEquipmentJobStarted]
      // The adapter really receives an uncorrelated reply and produces its
      // public OutcomeUnknown. Do not directly inject an invented StageError.
      equipment.expectMessageType[SimulateCommand].replyTo ! StatusReport("CDSEM-01", net.imadz.fab.protocol.Idle, None, Map.empty)
      val journal = PersistenceTestKit(system)
      eventually {
        val parentEvents = journal.persistedInStorage(s"FabPipelineExecution|$id")
        parentEvents.collect { case RecoveryRequired(_, error, _) => error.errorCode } shouldBe Seq("EQUIPMENT_OUTCOME_UNKNOWN")
        parentEvents.exists(_.isInstanceOf[StageFailed]) shouldBe false
        parentEvents.exists(_.isInstanceOf[OcapHandled]) shouldBe false
        parentEvents.exists(_.isInstanceOf[AllCompleted]) shouldBe false
        val childEvents = journal.persistedInStorage(s"FabPipelineExecutionRun|$id")
        childEvents.exists(_.isInstanceOf[PersistentMonarch.StageDone]) shouldBe false
        childEvents.exists(_.isInstanceOf[PersistentMonarch.RunFailedEvt]) shouldBe false
      }
      commands.expectNoMessage(300.millis)
      val status = createTestProbe[State]()
      parent ! GetExecutionState(status.ref)
      status.expectMessageType[AwaitingRecovery].planBytes should not be empty
      info("Equipment OutcomeUnknown requires reconciliation; no FailLot, OCAP, checkpoint or business terminal")
      testKit.stop(parent)
    }
  }
}
