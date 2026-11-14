package net.imadz.application.chain

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import akka.persistence.testkit.{PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin}
import akka.persistence.testkit.scaladsl.{PersistenceTestKit, SnapshotTestKit}
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import net.imadz.application.aggregates.LotProtocol._
import net.imadz.application.chain.FabExecutionModel._
import net.imadz.application.chain.FabPipelineExecutionActor._
import net.imadz.application.chain.FabScenarioPipeline.{Classify, Measure, PipelineStage, RunRecipe, SealComplete}
import net.imadz.application.scenario.StandardScenarios
import net.imadz.domain.events.{FabSimulationEvent, RecoveryEvent}
import net.imadz.domain.routing._
import net.imadz.fab.protocol._
import net.imadz.infrastructure.persistence.FabPipelineSerializer
import net.imadz.monarch.persistence.{MonarchSerializer, PersistentMonarch}
import org.scalatest.LoneElement
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._

object FabPipelineExecutionActorSpec {
  // Keep real application serializer bindings, but never start a cluster, bind
  // fixed ports, touch the demo's LevelDB directories, or connect to Mongo/JDBC.
  val config = ConfigFactory.parseString("""
    fab.persistence.read-journal-plugin-id = "akka.persistence.testkit.query"
    akka.actor.provider = local
    akka.actor.allow-java-serialization = off
    akka.remote.artery.canonical.port = 0
    akka.cluster.seed-nodes = []
    akka.persistence.testkit.events.serialize = true
    akka.persistence.testkit.snapshots.serialize = true
    akka.persistence.testkit.events.assert-timeout = 8s
    akka.persistence.testkit.snapshots.assert-timeout = 8s
  """).withFallback(PersistenceTestKitPlugin.config)
    .withFallback(PersistenceTestKitSnapshotPlugin.config)
    .withFallback(ConfigFactory.load())
}

/** Real parent + PersistentMonarch child; only external equipment/lot/Saga
  * boundaries are controlled. This is actor integration, not a JVM/HTTP/DB E2E. */
class FabPipelineExecutionActorSpec
    extends ScalaTestWithActorTestKit(FabPipelineExecutionActorSpec.config)
    with AnyWordSpecLike with LoneElement {

  private val journal = PersistenceTestKit(system)
  private val snapshots = SnapshotTestKit(system)
  private val waferId = "WAFER-1"
  private val initial = FabDemoState(Map(waferId -> WaferInfo(waferId)))
  private val parentType = "FabPipelineExecution"
  private val childType = "FabPipelineExecutionRun"
  private def parentId(id: String): String = s"$parentType|$id"
  private def childId(id: String): String = s"$childType|$id"
  private def uniqueId(): String = UUID.randomUUID().toString

  private def confirmingLot(commands: TestProbe[LotCommand]): Behavior[LotCommand] =
    Behaviors.receiveMessage { command =>
      commands.ref ! command
      command match {
        case RecordEquipmentJobStarted(_, _, reply) => reply ! LotConfirmation(None)
        case RecordEquipmentJobCompleted(_, _, _, reply) => reply ! LotConfirmation(None)
        case RecordWaferMeasured(_, _, reply) => reply ! LotConfirmation(None)
        case RecordWaferClassified(_, _, _, _, reply) => reply ! LotConfirmation(None)
        case RecordTransportCompleted(_, _, reply) => reply ! LotConfirmation(None)
        case SealLot(reply) => reply ! LotConfirmation(None)
        case CompleteProcess(_, _, _, _, reply) => reply ! LotConfirmation(None)
        case FailLot(_, _, reply) => reply ! LotConfirmation(None)
        case other => throw new IllegalArgumentException(s"Unexpected lot command: $other")
      }
      Behaviors.same
    }

  private final class Fixture(stages: Seq[PipelineStage], rules: List[OcapRuleDefinition] = Nil,
                              lotFailure: Option[net.imadz.common.CommonTypes.iMadzError] = None,
                              confirmLots: Boolean = true) {
    val id: String = uniqueId()
    val scenario = StandardScenarios.photoCell5Wafer.copy(scenarioId = s"compat-$id", lotSize = 1,
      waferIds = Seq(waferId))
    val events: TestProbe[FabSimulationEvent] = createTestProbe[FabSimulationEvent]()
    val lotCommands: TestProbe[LotCommand] = createTestProbe[LotCommand]()
    val equipment: TestProbe[SimulatorCommand] = createTestProbe[SimulatorCommand]()
    val lot = spawn(if (lotFailure.isEmpty && confirmLots) confirmingLot(lotCommands)
      else Behaviors.receiveMessage[LotCommand] { command =>
        lotCommands.ref ! command
        command match {
          case RecordEquipmentJobStarted(_, _, reply) if confirmLots => reply ! LotConfirmation(lotFailure)
          case RecordEquipmentJobCompleted(_, _, _, reply) if confirmLots => reply ! LotConfirmation(lotFailure)
          case RecordWaferMeasured(_, _, reply) if confirmLots => reply ! LotConfirmation(lotFailure)
          case _ => ()
        }
        Behaviors.same
      })
    val lotRef = TestEntityRef(EntityTypeKey[LotCommand]("test-lot"), id, lot)
    val rebuiltContexts = new AtomicInteger(0)

    def context(): FabDemoContext = {
      val adapter = new ActorEquipmentAdapter()(system, system.executionContext)
      adapter.registerSimulator("CDSEM-01", equipment.ref)
      adapter.registerSimulator("LITHO-01", equipment.ref)
      FabDemoContext(scenario, s"FOUP-$id", lotRef, lotRef,
        Map(waferId -> UUID.nameUUIDFromBytes(waferId.getBytes(java.nio.charset.StandardCharsets.UTF_8))),
        UUID.randomUUID(), UUID.randomUUID(), adapter, events.ref ! _,
        system.ignoreRef[LotConfirmation],
        (_, _, _, _, _) => Future.failed(new IllegalStateException("This scenario must not call Saga")),
        speedMultiplier = 100.0, ocapRules = rules)(system.executionContext)
    }

    def spawnParent(supervised: Boolean = false): ActorRef[Command] = {
      val behavior = FabPipelineExecutionActor(id,
        (scenarioId, workOrderId) => {
          scenarioId shouldBe scenario.scenarioId
          workOrderId shouldBe id
          rebuiltContexts.incrementAndGet()
          context()
        },
        _ => throw new IllegalStateException("Recovery must use persisted state, not the fallback factory"),
        _ => stages, events.ref ! _)(system.executionContext)
      spawn(if (supervised) Behaviors.supervise(behavior)
        .onFailure[RuntimeException](akka.actor.typed.SupervisorStrategy.restart) else behavior)
    }

    def start(parent: ActorRef[Command]): Unit = {
      val replies = createTestProbe[ExecutionReply]()
      parent ! StartExecution(scenario.scenarioId, id, initial, stages, context(), replies.ref)
      replies.expectMessage(Accepted)
      val accepted = journal.expectNextPersistedType[StartedWithInput](parentId(id))
      accepted.initialState shouldBe initial
      FabPipelinePlanCodec.decode(accepted.planBytes.toArray).toOption.get shouldBe stages.toVector
      val started = journal.expectNextPersistedType[PersistentMonarch.RunStarted](childId(id))
      started.stageCursors shouldBe stages.zipWithIndex.map { case (stage, index) =>
        s"${FabPipelineProcessor.stageName(stage)}#$index"
      }.toVector
      FabRunStateCodec.protobuf.decode(started.initialSnapshot).state shouldBe initial
    }

    def request(): SimulateCommand = equipment.expectMessageType[SimulateCommand](8.seconds)

    def respondMeasured(request: SimulateCommand): Unit = {
      request.cmd shouldBe ProcessRecipe("CD-MEASURE-001")
      request.replyTo ! JobCompleted("measure-job", "CDSEM-01",
        MetrologyResult("measure-job", Map(waferId -> CriticalDimension(waferId, 32.0))))
    }

    def parentEvents: Seq[Event] = journal.persistedInStorage(parentId(id)).map(_.asInstanceOf[Event])
    def childEvents: Seq[PersistentMonarch.Event] =
      journal.persistedInStorage(childId(id)).map(_.asInstanceOf[PersistentMonarch.Event])

    def awaitCompleted(): AllCompleted = {
      eventually(timeout(8.seconds), interval(50.millis)) {
        parentEvents.last shouldBe AllCompleted(scenario.scenarioId, id, 1, 1, 0, 0)
        childEvents.last shouldBe PersistentMonarch.RunCompleted(id)
      }
      parentEvents.last.asInstanceOf[AllCompleted]
    }
  }

  "Fab pipeline protobuf compatibility" should {
    "use the candidate serializers and round-trip every parent event and snapshot variant" in {
      val serialization = SerializationExtension(system.classicSystem)
      val err = StageError("Measure", Some("CDSEM-01"), "SENSOR_ANOMALY", "out of range")
      val rich = initial.copy(wafers = Map(waferId -> WaferInfo(waferId, 2, List(32.0, 35.0),
        Some("PASS"), Some("rwk"), Map("cd_nm" -> List(32.0, 35.0)))), passCount = 1,
        scrapCount = 2, iteration = 3, ledgerSeq = 4, pilotPassed = true, reviewApproved = true,
        spawnedChildLotKey = Some("rwk"), currentRoutingStep = 5,
        routingStepReentry = Map("LITHO" -> 2), areaVisitHistory = List("MET", "LITHO"), currentArea = "MET",
        childLotView = Map("rwk" -> ("MERGED", 1)),
        equipmentState = Map("CDSEM-01" -> EquipmentState("CDSEM-01", "Error", 1)),
        ocapActions = List("rule" -> OcapComposite(List(OcapHold(1, "review"), OcapRework("R", 2),
          OcapScrap("scrap"), OcapNotify("notify", "engineer"), OcapAdjustRecipe("R", 0.5)))))
      val values: Seq[AnyRef] = Seq(Started("scenario", "wo", 2), StageStarted("Measure#0", 1L),
        StageCompleted("Measure#0", 2L, Map("equipment" -> "CDSEM-01"), Some(rich)),
        StageCompleted("Classify#1", 3L, Map.empty, None), StageFailed("Measure#0", err, 4L),
        OcapHandled("Measure#0", err, 5L, rich), StageProgress("OCAP", "review", "Measure#0", 6L),
        AllCompleted("scenario", "wo", 4, 1, 1, 2), ExecutionFailed("Measure#0", "fatal"),
        FabPipelineExecutionActor.Idle, Executing("scenario", "wo", List("Measure#0"), 2, Some(rich)),
        RecoveryRequired("Measure#0", err, 7L),
        AwaitingRecovery(Executing("scenario", "wo", Nil, 2, Some(rich)), "Measure#0", err),
        AwaitingRecovery(Executing("scenario", "wo", Nil, 2, Some(rich)), "Measure#0", err,
          FabPipelinePlanCodec.encode(Seq(Measure("CDSEM-01"), Classify)).toOption.get.toVector),
        StartedWithInput("scenario", "wo", rich, Vector("Classify#0"),
          FabPipelinePlanCodec.encode(Seq(Classify)).toOption.get.toVector),
        RestoredExecution(Executing("scenario", "wo", Nil, 1, Some(rich)),
          FabPipelinePlanCodec.encode(Seq(Classify)).toOption.get.toVector),
        Executing("scenario", "wo", Nil), Completed("scenario", "wo"), Failed("Measure#0", "fatal"))
      values.foreach { value =>
        val serializer = serialization.findSerializerFor(value)
        serializer shouldBe a[FabPipelineSerializer]
        serialization.deserialize(serialization.serialize(value).get, serializer.identifier,
          Serializers.manifestFor(serializer, value)).get shouldBe value
      }
      serialization.findSerializerFor(PersistentMonarch.RunCompleted("wo")) shouldBe a[MonarchSerializer]
      serialization.findSerializerFor(PersistentMonarch.Idle) shouldBe a[MonarchSerializer]
      val persistenceLocation = classOf[MonarchSerializer].getProtectionDomain.getCodeSource.getLocation.toString
      val coreLocation = classOf[net.imadz.monarch.AsyncCheckpointHooks[_, _]].getProtectionDomain.getCodeSource.getLocation.toString
      persistenceLocation should endWith("/monarch-persistence_2.13-0.2.1.jar")
      coreLocation should endWith("/monarch-core_2.13-0.2.1.jar")
      Option(System.getProperty("monarch.repository")).foreach { repository =>
        persistenceLocation should startWith(repository.stripSuffix("/"))
        coreLocation should startWith(repository.stripSuffix("/"))
      }
      system.settings.config.getBoolean("akka.persistence.testkit.events.serialize") shouldBe true
      system.settings.config.getBoolean("akka.persistence.testkit.snapshots.serialize") shouldBe true
      val serializer = new FabPipelineSerializer
      intercept[IllegalArgumentException](serializer.manifest(StopPipeline("wo")))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "unknown"))
      intercept[IllegalArgumentException](serializer.fromBinary(Array.emptyByteArray, "fab-pipeline-event-v1"))
      info(s"Candidate runtime core=$coreLocation persistence=$persistenceLocation; parent serializer=${serializer.identifier}")
    }
  }

  "FabPipelineExecutionActor with Monarch 0.2.1" should {
    "complete actual measurement, classification and sealing with durable child checkpoints and parent snapshots" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), Classify, SealComplete))
      val parent = f.spawnParent()
      f.start(parent)
      f.respondMeasured(f.request())
      f.awaitCompleted()
      val checkpoints = f.childEvents.collect { case e: PersistentMonarch.StageDone => e }
      checkpoints.map(_.cursor) shouldBe Seq("Measure_CDSEM-01#0", "Classify#1", "SealComplete#2")
      val finalState = FabRunStateCodec.protobuf.decode(checkpoints.last.snapshot)
      finalState.completedCount shouldBe 3
      finalState.state.wafers(waferId).cdValueHistory shouldBe List(32.0)
      finalState.state.wafers(waferId).classification shouldBe Some("PASS")
      f.parentEvents.collect { case e: StageCompleted => e.phase } shouldBe checkpoints.map(_.cursor)
      f.parentEvents.collect { case e: OcapHandled => e } shouldBe empty
      eventually(timeout(8.seconds), interval(50.millis)) {
        snapshots.persistedInStorage(parentId(f.id)).exists { case (_, state) =>
          state match {
            case RestoredExecution(e, _) => e.fabDemoState.exists(_.wafers(waferId).cdValueHistory == List(32.0))
            case _ => false
          }
        } shouldBe true
      }
      val notifications = f.lotCommands.receiveMessages(7)
      notifications.collect { case e: CompleteProcess => e.passCount } shouldBe Seq(1)
      info(s"Happy path: ${checkpoints.map(_.cursor).mkString(", ")}; AllCompleted(1,1,0,0); serialized parent snapshot")
      testKit.stop(parent)
    }

    "checkpoint inline OCAP handling without recording a fake successful stage" in {
      val rule = OcapRuleDefinition("sensor", "Notify engineer", MeasurementCondition("missing", GreaterThan),
        OcapNotify("Sensor inspection required", "engineer"), onMissingMetric = ConservativeReject)
      val f = new Fixture(Seq(Measure("CDSEM-01"), Classify, SealComplete), List(rule))
      val parent = f.spawnParent()
      f.start(parent)
      f.request().replyTo ! JobFailed("bad-measure", "CDSEM-01", "SENSOR_ANOMALY", "sensor unavailable")
      f.awaitCompleted()
      val failure = f.parentEvents.collect { case e: StageFailed => e }.loneElement
      failure.error shouldBe StageError("Measure", Some("CDSEM-01"), "SENSOR_ANOMALY", "sensor unavailable")
      val handled = f.parentEvents.collect { case e: OcapHandled => e }.loneElement
      handled.phase shouldBe "Measure_CDSEM-01#0"
      handled.error shouldBe failure.error
      f.parentEvents.indexOf(failure) should be < f.parentEvents.indexOf(handled)
      f.parentEvents.collect { case e: StageCompleted => e.phase } shouldBe Seq("Classify#1", "SealComplete#2")
      val checkpoints = f.childEvents.collect { case e: PersistentMonarch.StageDone => e }
      val resolved = FabRunStateCodec.protobuf.decode(checkpoints.head.snapshot)
      resolved.completedCount shouldBe 1
      resolved.lastOutcome shouldBe Some(FabRunState.OcapResolved(failure.error))
      checkpoints.size shouldBe 3
      info("Inline OCAP: SENSOR_ANOMALY -> StageFailed -> OcapHandled; child StageDone advances to 1; no fake Measure success")
      testKit.stop(parent)
    }

    "preserve an unhandled equipment failure instead of calling a failed Lot OCAP-resolved" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), Classify))
      val parent = f.spawnParent()
      f.start(parent)
      f.request().replyTo ! JobFailed("measure-job", "CDSEM-01", "SENSOR_ANOMALY", "sensor unavailable")
      eventually(timeout(8.seconds), interval(50.millis)) {
        f.parentEvents.collect { case e: ExecutionFailed => e }.size shouldBe 1
        f.childEvents.collect { case e: PersistentMonarch.RunFailedEvt => e }.size shouldBe 1
      }
      f.parentEvents.collect { case e: OcapHandled => e } shouldBe empty
      f.parentEvents.collect { case e: AllCompleted => e } shouldBe empty
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e } shouldBe empty
      f.lotCommands.fishForMessage(3.seconds) {
        case _: FailLot => akka.actor.testkit.typed.scaladsl.FishingOutcomes.complete
        case _ => akka.actor.testkit.typed.scaladsl.FishingOutcomes.continue
      }
      testKit.stop(parent)
    }

    "not report committed parent progress or start the next stage when a child checkpoint is rejected" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), RunRecipe("LITHO-01", "R")))
      val parent = f.spawnParent()
      f.start(parent)
      val request = f.request()
      journal.rejectNextPersisted(childId(f.id))
      f.respondMeasured(request)
      // RunStarted was consumed by start(); StageDone must not reach storage.
      journal.expectNothingPersisted(childId(f.id), 1.second)
      f.equipment.expectNoMessage(500.millis)
      f.parentEvents.collect { case e: StageCompleted => e } shouldBe empty
      f.parentEvents.collect { case e: AllCompleted => e } shouldBe empty
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e } shouldBe empty
      f.parentEvents.collect { case e: StageStarted => e.phase } shouldBe Seq("Measure_CDSEM-01#0")
      eventually(timeout(8.seconds), interval(50.millis)) {
        f.parentEvents.collect { case e: RecoveryRequired => e }.loneElement.error.errorCode shouldBe "CHILD_STOPPED"
      }
      info("Rejected child StageDone: parent requires controlled recovery; no completion or downstream request")
      testKit.stop(parent)
    }

    "require controlled recovery when lot acknowledgement is unknown without writing business failure" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), Classify), confirmLots = false)
      val parent = f.spawnParent()
      f.start(parent)
      f.respondMeasured(f.request())
      eventually(timeout(8.seconds), interval(50.millis)) {
        val required = f.parentEvents.collect { case e: RecoveryRequired => e }.loneElement
        required.error.errorCode shouldBe "LOT_ACK_TIMEOUT"
      }
      val replies = createTestProbe[State]()
      parent ! GetExecutionState(replies.ref)
      val paused = replies.expectMessageType[AwaitingRecovery]
      paused.execution.fabDemoState shouldBe Some(initial)
      FabPipelinePlanCodec.decode(paused.planBytes.toArray).toOption.get shouldBe Seq(Measure("CDSEM-01"), Classify)
      testKit.stop(parent)
      val recovered = f.spawnParent()
      recovered ! GetExecutionState(replies.ref)
      replies.expectMessage(paused)
      f.equipment.expectNoMessage(300.millis)
      val serialization = SerializationExtension(system.classicSystem)
      val serializer = serialization.findSerializerFor(paused)
      serialization.deserialize(serialization.serialize(paused).get, serializer.identifier,
        Serializers.manifestFor(serializer, paused)).get shouldBe paused
      testKit.stop(recovered)
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e } shouldBe empty
      f.childEvents.collect { case e: PersistentMonarch.RunFailedEvt => e } shouldBe empty
      f.parentEvents.collect { case e: ExecutionFailed => e } shouldBe empty
      f.parentEvents.collect { case e: OcapHandled => e } shouldBe empty
      testKit.stop(parent)
    }

    "preserve explicit lot rejection rather than running an OCAP recovery" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), Classify),
        lotFailure = Some(net.imadz.common.CommonTypes.iMadzError("LOT_HELD", "review required")))
      val parent = f.spawnParent()
      f.start(parent)
      f.respondMeasured(f.request())
      eventually(timeout(8.seconds), interval(50.millis)) {
        f.parentEvents.collect { case e: StageFailed => e }.loneElement.error.errorCode shouldBe "LOT_ACK_REJECTED"
        f.childEvents.collect { case e: PersistentMonarch.RunFailedEvt => e }.size shouldBe 1
      }
      f.parentEvents.collect { case e: OcapHandled => e } shouldBe empty
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e } shouldBe empty
      testKit.stop(parent)
    }

    "recover accepted input when the parent committed start but the child never started" in {
      val stages = Seq(Measure("CDSEM-01"), Classify, SealComplete)
      val f = new Fixture(stages)
      val plan = stages.zipWithIndex.map { case (stage, index) => s"${FabPipelineProcessor.stageName(stage)}#$index" }.toVector
      val bytes = FabPipelinePlanCodec.encode(stages).toOption.get.toVector
      journal.persistForRecovery(parentId(f.id), List(StartedWithInput(f.scenario.scenarioId, f.id, initial, plan, bytes)))
      val parent = f.spawnParent()
      val request = f.request()
      val started = journal.persistedInStorage(childId(f.id)).collect { case e: PersistentMonarch.RunStarted => e }.loneElement
      FabRunStateCodec.protobuf.decode(started.initialSnapshot).state shouldBe initial
      f.respondMeasured(request)
      f.awaitCompleted()
      testKit.stop(parent)
    }

    "reconcile a committed child checkpoint missing from the parent before resuming equipment" in {
      val stages = Seq(Measure("CDSEM-01"), RunRecipe("LITHO-01", "R"), Classify, SealComplete)
      val f = new Fixture(stages)
      val plan = stages.zipWithIndex.map { case (stage, index) => s"${FabPipelineProcessor.stageName(stage)}#$index" }.toVector
      val initialRun = FabRunState.initial(f.scenario.scenarioId, f.id, initial)
      val measured = initial.copy(wafers = Map(waferId -> WaferInfo(waferId, cdValueHistory = List(32.0))))
      val checkpoint = initialRun.copy(state = measured, completedCount = 1)
      journal.persistForRecovery(parentId(f.id), List(StartedWithInput(f.scenario.scenarioId, f.id, initial,
        plan, FabPipelinePlanCodec.encode(stages).toOption.get.toVector)))
      journal.persistForRecovery(childId(f.id), List(PersistentMonarch.RunStarted(f.id, plan,
        FabRunStateCodec.protobuf.encode(initialRun)), PersistentMonarch.StageDone(plan.head, 1L, Map.empty,
        FabRunStateCodec.protobuf.encode(checkpoint))))
      val parent = f.spawnParent()
      val request = f.request()
      request.cmd shouldBe ProcessRecipe("R")
      f.parentEvents.collect { case e: StageCompleted => e.phase } shouldBe Seq(plan.head)
      request.replyTo ! JobCompleted("restored-job", "LITHO-01", LithoExposureResult("restored-job", "R", Map.empty))
      f.awaitCompleted()
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e.cursor }.count(_ == plan.head) shouldBe 1
      testKit.stop(parent)
    }

    "reconcile child success and failure terminals without re-executing stages" in {
      Seq(true, false).foreach { success =>
        val stages = Seq(Measure("CDSEM-01"))
        val f = new Fixture(stages)
        val plan = Vector("Measure_CDSEM-01#0")
        val initialRun = FabRunState.initial(f.scenario.scenarioId, f.id, initial)
        val finalState = initialRun.copy(state = initial.copy(wafers = Map(waferId ->
          WaferInfo(waferId, classification = Some("PASS")))), completedCount = 1)
        journal.persistForRecovery(parentId(f.id), List(StartedWithInput(f.scenario.scenarioId, f.id, initial,
          plan, FabPipelinePlanCodec.encode(stages).toOption.get.toVector)))
        val events: List[PersistentMonarch.Event] = if (success) List(
          PersistentMonarch.RunStarted(f.id, plan, FabRunStateCodec.protobuf.encode(initialRun)),
          PersistentMonarch.StageDone(plan.head, 1L, Map.empty, FabRunStateCodec.protobuf.encode(finalState)),
          PersistentMonarch.RunCompleted(f.id))
        else List(PersistentMonarch.RunStarted(f.id, plan, FabRunStateCodec.protobuf.encode(initialRun)),
          PersistentMonarch.RunFailedEvt(plan.head, "SENSOR_ANOMALY"))
        journal.persistForRecovery(childId(f.id), events)
        val parent = f.spawnParent()
        eventually(timeout(8.seconds), interval(50.millis)) {
          if (success) f.parentEvents.last shouldBe AllCompleted(f.scenario.scenarioId, f.id, 1, 1, 0, 0)
          else f.parentEvents.last shouldBe ExecutionFailed(plan.head, "SENSOR_ANOMALY")
        }
        f.equipment.expectNoMessage(300.millis)
        f.rebuiltContexts.get() shouldBe 0
        val serializer = SerializationExtension(system.classicSystem)
        f.childEvents.map(event => serializer.serialize(event).get.toVector) shouldBe
          events.map(event => serializer.serialize(event).get.toVector).toVector
        testKit.stop(parent)
      }
    }

    "recover the same supervised parent reference and ignore unowned progress after restart" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), RunRecipe("LITHO-01", "R"), Classify, SealComplete))
      val parent = f.spawnParent(supervised = true)
      f.start(parent)
      f.respondMeasured(f.request())
      val staleRequest = f.request()
      parent ! StopPipeline(f.id)
      val resumed = f.request()
      resumed.cmd shouldBe ProcessRecipe("R")
      parent ! StageProgressEvent("FAKE", "unowned callback", "Measure_CDSEM-01#0")
      staleRequest.replyTo ! JobCompleted("stale-job", "LITHO-01", LithoExposureResult("stale-job", "R", Map.empty))
      resumed.replyTo ! JobCompleted("resumed-job", "LITHO-01", LithoExposureResult("resumed-job", "R", Map.empty))
      f.awaitCompleted()
      f.parentEvents.collect { case StageProgress("FAKE", _, _, _) => 1 } shouldBe empty
      f.childEvents.collect { case e: PersistentMonarch.StageDone => e.cursor } shouldBe
        Seq("Measure_CDSEM-01#0", "RunRecipe_LITHO-01_R#1", "Classify#2", "SealComplete#3")
      testKit.stop(parent)
    }

    "recover both real actors from serialized journals and a parent snapshot, skipping committed work and fencing a stale reply" in {
      val f = new Fixture(Seq(Measure("CDSEM-01"), RunRecipe("LITHO-01", "R"), Classify, SealComplete))
      val parent = f.spawnParent()
      f.start(parent)
      f.respondMeasured(f.request())
      val staleRequest = f.request() // keep the uncommitted second stage in flight
      staleRequest.cmd shouldBe ProcessRecipe("R")
      eventually(timeout(8.seconds), interval(50.millis)) {
        f.parentEvents.collect { case e: StageCompleted => e.phase } shouldBe Seq("Measure_CDSEM-01#0")
        snapshots.persistedInStorage(parentId(f.id)) should not be empty
      }
      testKit.stop(parent) // stops the actual child, not an import-and-call surrogate
      val recoveredParent = f.spawnParent()
      val resumedRequest = f.request()
      resumedRequest.cmd shouldBe ProcessRecipe("R") // Measure is not run again
      f.rebuiltContexts.get() shouldBe 1
      resumedRequest.replyTo ! JobCompleted("resumed-job", "LITHO-01", LithoExposureResult("resumed-job", "R", Map.empty))
      f.awaitCompleted()
      val parentCount = f.parentEvents.size
      val childCount = f.childEvents.size
      staleRequest.replyTo ! JobCompleted("stale-job", "LITHO-01", LithoExposureResult("stale-job", "R", Map.empty))
      f.equipment.expectNoMessage(500.millis)
      f.childEvents.size shouldBe childCount
      f.parentEvents.size shouldBe parentCount
      val checkpoints = f.childEvents.collect { case e: PersistentMonarch.StageDone => e }
      checkpoints.map(_.cursor) shouldBe Seq("Measure_CDSEM-01#0", "RunRecipe_LITHO-01_R#1", "Classify#2", "SealComplete#3")
      f.childEvents.count(_.isInstanceOf[PersistentMonarch.RunStarted]) shouldBe 1
      f.parentEvents.count(_.isInstanceOf[StartedWithInput]) shouldBe 1
      val finalState = FabRunStateCodec.protobuf.decode(checkpoints.last.snapshot)
      finalState.state.wafers(waferId).cdValueHistory shouldBe List(32.0)
      finalState.state.wafers(waferId).classification shouldBe Some("PASS")
      // The recovery observer reaches the parent's real publishing boundary.
      f.events.fishForMessage(3.seconds) {
        case RecoveryEvent(_, "RECOVERED", _, 1, _, _) => akka.actor.testkit.typed.scaladsl.FishingOutcomes.complete
        case _ => akka.actor.testkit.typed.scaladsl.FishingOutcomes.continue
      }
      info("Recovery: serialized Measure checkpoint/snapshot replayed; Process retried; Measure skipped; stale old reply produced no new records")
      testKit.stop(recoveredParent)
    }
  }
}
