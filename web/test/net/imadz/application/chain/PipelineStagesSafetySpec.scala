package net.imadz.application.chain

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.testkit.scaladsl.TestEntityRef
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import com.typesafe.config.ConfigFactory
import net.imadz.application.actor.EquipmentAreaActor
import net.imadz.application.aggregates.LotProtocol._
import net.imadz.application.chain.FabExecutionModel._
import net.imadz.application.scenario.StandardScenarios
import net.imadz.domain.events.{FabSimulationEvent, PipelineStageFailed, ProcessingCompleted, ProcessingStarted}
import net.imadz.fab.protocol._
import net.imadz.fab.simulation.{CdSemConfig, CdSemSimulator, EquipmentConfig}
import net.imadz.monarch.StaleRun
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

/** Real equipment actor and real area actor negatives; controlled I/O probes
  * assert observable commands, not a second copy of the production algorithm.
  * Local testkit only: never touches the application's journal or fixed ports.
  */
class PipelineStagesSafetySpec extends ScalaTestWithActorTestKit(
  ConfigFactory.parseString("""
    akka.actor.provider = local
    akka.actor.allow-java-serialization = off
    akka.persistence.testkit.events.serialize = false
  """).withFallback(PersistenceTestKitPlugin.config).withFallback(ConfigFactory.load()))
    with AnyWordSpecLike {

  private val initial = FabDemoState(Map("W1" -> WaferInfo("W1")))

  private final class Fixture {
    val fresh = new AtomicBoolean(true)
    val events = createTestProbe[FabSimulationEvent]()
    val lots = createTestProbe[LotCommand]()
    val equipment = createTestProbe[SimulatorCommand]()
    val areas = createTestProbe[EquipmentAreaActor.Command]()
    val progress = createTestProbe[(String, String, String)]()
    val adapter = new ActorEquipmentAdapter()(system, system.executionContext)
    adapter.registerSimulator("CDSEM-01", equipment.ref)
    adapter.registerSimulator("LITHO-01", equipment.ref)
    adapter.registerSimulator("AMHS", equipment.ref)
    val lotId = UUID.randomUUID()
    val lotRef = TestEntityRef(EntityTypeKey[LotCommand]("safety-lot"), lotId.toString, lots.ref)
    val scenario = StandardScenarios.photoCell5Wafer.copy(waferIds = Seq("W1"), lotSize = 1)

    def context(withArea: Boolean = false): FabDemoContext = FabDemoContext(
      scenario, s"FOUP-$lotId", lotRef, lotRef, Map("W1" -> UUID.randomUUID()),
      lotId, UUID.randomUUID(), adapter, events.ref ! _, system.ignoreRef[LotConfirmation],
      (_, _, _, _, _) => Future.failed(new IllegalStateException("No Saga in safety fixture")),
      speedMultiplier = 100.0, runToken = () => fresh.get(),
      areaActorOf = _ => if (withArea) Some(TestEntityRef(EquipmentAreaActor.EntityKey,
        s"safety-area-$lotId", areas.ref)) else None)(system.executionContext)

  }

  private def failure(result: Future[_]): Throwable = Await.result(result.failed, 4.seconds)

  /** Controlled adapter boundary: exercise synchronous throws and asynchronous
    * failures through the real stages without changing the Area/receipt protocol. */
  private final class ControlledAdapter(reply: () => Future[EquipmentEvent])
      extends ActorEquipmentAdapter()(system, system.executionContext) {
    val commands = createTestProbe[(String, EquipmentCommand)]()
    override def sendCommand(equipmentId: String, command: EquipmentCommand): Future[EquipmentEvent] = {
      commands.ref ! ((equipmentId, command))
      reply()
    }
  }

  "PipelineStages observer and send boundary" should {
    "keep an unknown result despite throwing publish and progress observers" in {
      val f = new Fixture
      val published = new AtomicInteger(0)
      val progressed = new AtomicInteger(0)
      val ctx = f.context(withArea = true).copy(publisher = event => {
        f.events.ref ! event
        published.incrementAndGet()
        throw new IllegalStateException("publish observer failed")
      })(system.executionContext)
      ctx.stageProgressFn = (status, detail, phase) => {
        f.progress.ref ! ((status, detail, phase))
        progressed.incrementAndGet()
        throw new IllegalArgumentException("progress observer failed")
      }
      val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      f.equipment.expectMessageType[SimulateCommand].replyTo ! JobCompleted("j", "CDSEM-01",
        MetrologyResult("j", Map.empty))
      failure(result).asInstanceOf[PipelineStages.RecoveryRequired].errorCode shouldBe "EQUIPMENT_REPLY_UNKNOWN"
      f.progress.receiveMessages(2).map(_._1) shouldBe Seq("MEASURING", "RECOVERY_REQUIRED")
      published.get() shouldBe 2
      progressed.get() shouldBe 2
      val events = f.events.receiveMessages(2)
      events.exists(_.isInstanceOf[ProcessingStarted]) shouldBe true
      events.exists(_.isInstanceOf[PipelineStageFailed]) shouldBe false
      events.exists(_.isInstanceOf[ProcessingCompleted]) shouldBe false
      f.lots.expectNoMessage(150.millis)
      f.areas.expectNoMessage(150.millis)
      f.equipment.expectNoMessage(150.millis)
      f.events.expectNoMessage(150.millis)
    }

    "keep a correlated device failure and ReportFault despite throwing observers" in {
      val f = new Fixture
      val ctx = f.context(withArea = true).copy(publisher = event => {
        f.events.ref ! event
        throw new IllegalStateException("publish observer failed")
      })(system.executionContext)
      ctx.stageProgressFn = (_, _, _) => throw new IllegalArgumentException("progress observer failed")
      val result = PipelineStages.process(initial, ctx, "LITHO-01", "R", "LITHO")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      val failed = JobFailed("device-job", "LITHO-01", FaultType.HardwareFault, "real device fault")
      f.equipment.expectMessageType[SimulateCommand].replyTo ! failed
      failure(result).asInstanceOf[StageFailedException].error shouldBe
        StageError("Process", Some("LITHO-01"), failed.errorCode, failed.detail)
      f.areas.expectMessage(EquipmentAreaActor.ReportFault("LITHO-01", failed.errorCode, failed.detail))
      val events = f.events.receiveMessages(4)
      events.collect { case event: PipelineStageFailed => event.errorCode } shouldBe Seq(failed.errorCode)
      events.collect { case event: ProcessingCompleted => event.success } shouldBe Seq(false)
      f.lots.expectNoMessage(150.millis)
      f.areas.expectNoMessage(150.millis)
      f.equipment.expectNoMessage(150.millis)
      f.events.expectNoMessage(150.millis)
    }

    "retain ordinary synchronous and asynchronous send failures as outcome unknown" in {
      Seq(false, true).foreach { synchronous =>
        val f = new Fixture
        val cause = new IllegalStateException(if (synchronous) "synchronous send failure" else "asynchronous send failure")
        val reply = Promise[EquipmentEvent]()
        val adapter = new ControlledAdapter(() => if (synchronous) throw cause else reply.future)
        val ctx = f.context(withArea = true).copy(adapter = adapter)(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => {
          f.progress.ref ! ((status, detail, phase))
          throw new IllegalArgumentException("progress observer failed")
        }
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
          EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        adapter.commands.expectMessage(("CDSEM-01", ProcessRecipe("CD-MEASURE-001")))
        if (!synchronous) reply.failure(cause)
        val error = failure(result).asInstanceOf[PipelineStages.RecoveryRequired]
        error.errorCode shouldBe "EQUIPMENT_OUTCOME_UNKNOWN"
        error.underlying should be theSameInstanceAs cause
        error.getCause should be theSameInstanceAs cause
        val progress = f.progress.receiveMessages(2)
        progress.map(_._1) shouldBe Seq("MEASURING", "RECOVERY_REQUIRED")
        progress.last._2 should include("EQUIPMENT_OUTCOME_UNKNOWN")
        progress.last._3 shouldBe "PhaseWaitRecovery"
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        adapter.commands.expectNoMessage(100.millis)
      }
    }

    "preserve every typed transport category and cause despite a throwing recovery observer" in {
      val cause = new IllegalStateException("lost reply")
      val errors = Seq[(Throwable, String)](
        ActorEquipmentAdapter.OutcomeUnknown("CDSEM-01", ProcessRecipe("CD-MEASURE-001"), cause) -> "EQUIPMENT_OUTCOME_UNKNOWN",
        ActorEquipmentAdapter.JobBusy("CDSEM-01", StatusReport("CDSEM-01", Busy, Some("old-job"), Map.empty)) -> "EQUIPMENT_JOB_BUSY",
        ActorEquipmentAdapter.SimulatorUnavailable("CDSEM-01") -> "EQUIPMENT_UNAVAILABLE")
      errors.foreach { case (error, code) =>
        val f = new Fixture
        val reply = Promise[EquipmentEvent]()
        val adapter = new ControlledAdapter(() => reply.future)
        val ctx = f.context(withArea = true).copy(adapter = adapter)(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => {
          f.progress.ref ! ((status, detail, phase))
          if (status == "RECOVERY_REQUIRED") throw new IllegalStateException("recovery observer failed")
        }
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
          EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        adapter.commands.expectMessage(("CDSEM-01", ProcessRecipe("CD-MEASURE-001")))
        reply.failure(error)
        failure(result) should be theSameInstanceAs error
        val progress = f.progress.receiveMessages(2)
        progress.last._1 shouldBe "RECOVERY_REQUIRED"
        progress.last._2 should include(code)
        progress.last._3 shouldBe "PhaseWaitRecovery"
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        adapter.commands.expectNoMessage(100.millis)
      }
    }

    "stop subsequent I/O when either observer revokes the token, even while throwing" in {
      for (publishing <- Seq(false, true); throwing <- Seq(false, true)) {
        val f = new Fixture
        val calls = new AtomicInteger(0)
        val ctx = f.context(withArea = true).copy(publisher = event => {
          f.events.ref ! event
          if (publishing && event.isInstanceOf[ProcessingStarted]) {
            calls.incrementAndGet()
            f.fresh.set(false)
            if (throwing) throw new IllegalStateException("publish revoked ownership")
          }
        })(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => {
          f.progress.ref ! ((status, detail, phase))
          if (!publishing) {
            calls.incrementAndGet()
            f.fresh.set(false)
            if (throwing) throw new IllegalStateException("progress revoked ownership")
          }
        }
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        if (publishing) {
          f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
            EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
          f.lots.expectMessageType[RecordEquipmentJobStarted]
        }
        failure(result) should be theSameInstanceAs StaleRun
        calls.get() shouldBe 1
        f.progress.receiveMessages(1).head._1 shouldBe "MEASURING"
        f.events.receiveMessages(if (publishing) 2 else 1)
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        f.equipment.expectNoMessage(100.millis)
        f.events.expectNoMessage(100.millis)
        f.progress.expectNoMessage(100.millis)
      }
    }

    "propagate observer StaleRun rather than swallowing it as NonFatal" in {
      Seq(false, true).foreach { publishing =>
        val f = new Fixture
        val ctx = f.context(withArea = true).copy(publisher = event => {
          f.events.ref ! event
          if (publishing) throw StaleRun
        })(system.executionContext)
        ctx.stageProgressFn = (_, _, _) => throw StaleRun
        failure(PipelineStages.measure(initial, ctx, "CDSEM-01")) should be theSameInstanceAs StaleRun
        f.fresh.get() shouldBe true // proves preservation, not just a revoked-token post-check
        f.events.receiveMessages(1)
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        f.equipment.expectNoMessage(100.millis)
        f.events.expectNoMessage(100.millis)
      }
    }

    "propagate synchronous and asynchronous adapter StaleRun while the token is still fresh" in {
      Seq(false, true).foreach { synchronous =>
        val f = new Fixture
        val reply = Promise[EquipmentEvent]()
        val adapter = new ControlledAdapter(() => if (synchronous) throw StaleRun else reply.future)
        val ctx = f.context(withArea = true).copy(adapter = adapter)(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => f.progress.ref ! ((status, detail, phase))
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
          EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        adapter.commands.expectMessage(("CDSEM-01", ProcessRecipe("CD-MEASURE-001")))
        if (!synchronous) reply.failure(StaleRun)
        failure(result) should be theSameInstanceAs StaleRun
        f.fresh.get() shouldBe true
        f.progress.receiveMessages(1).head._1 shouldBe "MEASURING"
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        adapter.commands.expectNoMessage(100.millis)
        f.progress.expectNoMessage(100.millis)
      }
    }

    "not publish completion or ReportFault after a failure observer revokes ownership" in {
      val f = new Fixture
      val ctx = f.context(withArea = true).copy(publisher = event => {
        f.events.ref ! event
        if (event.isInstanceOf[PipelineStageFailed]) {
          f.fresh.set(false)
          throw new IllegalStateException("failure observer revoked ownership")
        }
      })(system.executionContext)
      ctx.stageProgressFn = (_, _, _) => ()
      val result = PipelineStages.process(initial, ctx, "LITHO-01", "R", "LITHO")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      f.equipment.expectMessageType[SimulateCommand].replyTo !
        JobFailed("device-job", "LITHO-01", FaultType.HardwareFault, "real device fault")
      failure(result) should be theSameInstanceAs StaleRun
      val events = f.events.receiveMessages(3)
      events.last shouldBe a[PipelineStageFailed]
      events.exists(_.isInstanceOf[ProcessingCompleted]) shouldBe false
      f.lots.expectNoMessage(150.millis)
      f.areas.expectNoMessage(150.millis)
      f.equipment.expectNoMessage(150.millis)
      f.events.expectNoMessage(150.millis)
    }

    "let revoked ownership supersede both ordinary and typed late send failures" in {
      val errors = Seq[Throwable](
        new IllegalStateException("late ordinary failure"),
        ActorEquipmentAdapter.OutcomeUnknown("CDSEM-01", ProcessRecipe("CD-MEASURE-001"),
          new IllegalArgumentException("late lost reply")), StaleRun)
      errors.foreach { error =>
        val f = new Fixture
        val reply = Promise[EquipmentEvent]()
        val adapter = new ControlledAdapter(() => reply.future)
        val ctx = f.context(withArea = true).copy(adapter = adapter)(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => f.progress.ref ! ((status, detail, phase))
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
          EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        adapter.commands.expectMessage(("CDSEM-01", ProcessRecipe("CD-MEASURE-001")))
        f.fresh.set(false)
        reply.failure(error)
        failure(result) should be theSameInstanceAs StaleRun
        f.progress.receiveMessages(1).head._1 shouldBe "MEASURING"
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        adapter.commands.expectNoMessage(100.millis)
        f.progress.expectNoMessage(100.millis)
      }
    }

    "fence the send recovery continuation when its observer revokes ownership" in {
      val errors = Seq[Throwable](
        new IllegalStateException("send failure"),
        ActorEquipmentAdapter.JobBusy("CDSEM-01", StatusReport("CDSEM-01", Busy, Some("old-job"), Map.empty)))
      errors.foreach { error =>
        val f = new Fixture
        val reply = Promise[EquipmentEvent]()
        val adapter = new ControlledAdapter(() => reply.future)
        val ctx = f.context(withArea = true).copy(adapter = adapter)(system.executionContext)
        ctx.stageProgressFn = (status, detail, phase) => {
          f.progress.ref ! ((status, detail, phase))
          if (status == "RECOVERY_REQUIRED") {
            f.fresh.set(false)
            throw new IllegalStateException("recovery observer revoked ownership")
          }
        }
        val result = PipelineStages.measure(initial, ctx, "CDSEM-01")
        f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
          EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        adapter.commands.expectMessage(("CDSEM-01", ProcessRecipe("CD-MEASURE-001")))
        reply.failure(error)
        failure(result) should be theSameInstanceAs StaleRun
        f.progress.receiveMessages(2).map(_._1) shouldBe Seq("MEASURING", "RECOVERY_REQUIRED")
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
        adapter.commands.expectNoMessage(100.millis)
        f.progress.expectNoMessage(100.millis)
      }
    }
  }

  "PipelineStages ownership boundary" should {
    "reject every side-effecting stage when already stale without any I/O" in {
      val f = new Fixture
      f.fresh.set(false)
      val ctx = f.context(withArea = true)
      val stages = Seq[() => Future[FabDemoState]](
        () => PipelineStages.loadFoup(initial, ctx),
        () => PipelineStages.transport(initial, ctx, "A", "B"),
        () => PipelineStages.trackIn(initial, ctx, "CDSEM-01"),
        () => PipelineStages.atEquipment(initial, ctx, "MET", "CDSEM-01"),
        () => PipelineStages.process(initial, ctx, "LITHO-01", "R", "LITHO"),
        () => PipelineStages.trackOut(initial, ctx, "CDSEM-01"),
        () => PipelineStages.measure(initial, ctx, "CDSEM-01"),
        () => PipelineStages.sealComplete(initial, ctx))
      stages.foreach(stage => failure(stage()) should be theSameInstanceAs StaleRun)
      f.lots.expectNoMessage(150.millis)
      f.areas.expectNoMessage(150.millis)
      f.equipment.expectNoMessage(150.millis)
      f.events.expectNoMessage(150.millis)
    }

    "fence a stale area reply before dispatch or Lot job-start" in {
      val f = new Fixture
      val result = PipelineStages.measure(initial, f.context(withArea = true), "CDSEM-01")
      val request = f.areas.expectMessageType[EquipmentAreaActor.StartProcess]
      f.fresh.set(false)
      request.replyTo.get ! EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      failure(result) should be theSameInstanceAs StaleRun
      f.equipment.expectNoMessage(200.millis)
      f.lots.expectNoMessage(200.millis)
      f.areas.expectNoMessage(200.millis)
    }

    "fence the real old simulator completion before Lot completion and FinishProcess" in {
      val f = new Fixture
      val simulator = spawn(new CdSemSimulator(CdSemConfig(waferIds = Seq("W1"),
        waferOutcomes = Map("W1" -> "PASS"))).apply(EquipmentConfig("CDSEM-01", "MET", 600.millis)))
      f.adapter.registerSimulator("CDSEM-01", simulator)
      val result = PipelineStages.measure(initial, f.context(withArea = true), "CDSEM-01")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      eventually { Await.result(f.adapter.queryStatus("CDSEM-01"), 2.seconds).status shouldBe Busy }
      f.fresh.set(false)
      failure(result) should be theSameInstanceAs StaleRun
      Await.result(f.adapter.queryStatus("CDSEM-01"), 2.seconds).status shouldBe Idle
      f.lots.expectNoMessage(250.millis)
      f.areas.expectNoMessage(250.millis)
      val published = f.events.receiveMessages(3).toVector
      published.exists(_.isInstanceOf[ProcessingCompleted]) shouldBe false
      f.events.expectNoMessage(150.millis)
      testKit.stop(simulator)
    }

    "fence both old failure replies and transport completion without success" in {
      val f = new Fixture
      val result = PipelineStages.process(initial, f.context(withArea = true), "LITHO-01", "R", "LITHO")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      val request = f.equipment.expectMessageType[SimulateCommand]
      f.fresh.set(false)
      request.replyTo ! JobFailed("old", "LITHO-01", FaultType.HardwareFault, "old callback")
      failure(result) should be theSameInstanceAs StaleRun
      f.areas.expectNoMessage(200.millis)
      f.lots.expectNoMessage(200.millis)

      val t = new Fixture
      val transported = PipelineStages.transport(initial, t.context(), "A", "B")
      val transfer = t.equipment.expectMessageType[SimulateCommand]
      t.fresh.set(false)
      transfer.replyTo ! FoupArrived(t.context().foupId, "B")
      failure(transported) should be theSameInstanceAs StaleRun
    }
  }

  "PipelineStages admission and response boundary" should {
    "not dispatch or repeatedly enqueue when the actual AreaActor returns QUEUED" in {
      val f = new Fixture
      val areaId = s"safety-${UUID.randomUUID()}"
      val area = spawn(EquipmentAreaActor(areaId, f.events.ref ! _))
      val ctx = f.context().copy(areaActorOf = _ => Some(TestEntityRef(EquipmentAreaActor.EntityKey, areaId, area)))(system.executionContext)
      val error = failure(PipelineStages.measure(initial, ctx, "CDSEM-01")).asInstanceOf[PipelineStages.RecoveryRequired]
      error.errorCode shouldBe "AREA_QUEUED_NOT_GRANTED"
      f.equipment.expectNoMessage(1.second)
      f.lots.expectNoMessage(200.millis)
      val journal = PersistenceTestKit(system)
      val entries = journal.persistedInStorage(s"EquipmentArea|$areaId")
      entries.collect { case e: EquipmentAreaActor.Enqueued => e }.size shouldBe 1
      entries.collect { case _: EquipmentAreaActor.Transitioned => 1 } shouldBe empty
      testKit.stop(area)
    }

    "keep unexpected responses unknown rather than advancing or reporting a device fault" in {
      val f = new Fixture
      val result = PipelineStages.measure(initial, f.context(withArea = true), "CDSEM-01")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      f.equipment.expectMessageType[SimulateCommand].replyTo ! JobCompleted("wrong-job", "OTHER",
        MetrologyResult("wrong-job", Map("W1" -> CriticalDimension("W1", 32.0))))
      val unknown = failure(result).asInstanceOf[ActorEquipmentAdapter.OutcomeUnknown]
      unknown.equipmentId shouldBe "CDSEM-01"
      unknown.command shouldBe ProcessRecipe("CD-MEASURE-001")
      unknown.getCause shouldBe a[IllegalStateException]
      f.lots.expectNoMessage(200.millis)
      f.areas.expectNoMessage(200.millis)
    }

    "reject a new measure on the same actual busy simulator without changing the old job" in {
      val old = new Fixture
      val simulator = spawn(new CdSemSimulator(CdSemConfig(waferIds = Seq("W1"),
        waferOutcomes = Map("W1" -> "PASS"))).apply(EquipmentConfig("CDSEM-01", "MET", 700.millis)))
      old.adapter.registerSimulator("CDSEM-01", simulator)
      val oldResult = PipelineStages.measure(initial, old.context(), "CDSEM-01")
      old.lots.expectMessageType[RecordEquipmentJobStarted]
      eventually { Await.result(old.adapter.queryStatus("CDSEM-01"), 2.seconds).status shouldBe Busy }
      val currentJob = Await.result(old.adapter.queryStatus("CDSEM-01"), 2.seconds).currentJob
      val recovered = new Fixture
      recovered.adapter.registerSimulator("CDSEM-01", simulator) // SAME actor, not a fake Busy oracle
      val newResult = PipelineStages.measure(initial, recovered.context(), "CDSEM-01")
      failure(newResult).asInstanceOf[ActorEquipmentAdapter.JobBusy].status.currentJob shouldBe currentJob
      old.fresh.set(false)
      recovered.lots.expectMessageType[RecordEquipmentJobStarted]
      recovered.lots.expectNoMessage(150.millis)
      recovered.areas.expectNoMessage(150.millis)
      failure(oldResult) should be theSameInstanceAs StaleRun
      old.lots.expectNoMessage(150.millis)
      testKit.stop(simulator)
    }

    "keep wrong result identities and missing measurements unknown with no completion writes" in {
      val replies = Seq[EquipmentEvent](
        JobCompleted("j", "CDSEM-01", LithoExposureResult("j", "R", Map.empty)),
        JobCompleted("j", "CDSEM-01", MetrologyResult("different", Map("W1" -> CriticalDimension("W1", 32.0)))),
        JobCompleted("j", "CDSEM-01", MetrologyResult("j", Map.empty)),
        JobCompleted("j", "CDSEM-01", MetrologyResult("j", Map("W1" -> CriticalDimension("W1", Double.NaN)))))
      replies.foreach { reply =>
        val f = new Fixture
        val result = PipelineStages.measure(initial, f.context(), "CDSEM-01")
        f.lots.expectMessageType[RecordEquipmentJobStarted]
        f.equipment.expectMessageType[SimulateCommand].replyTo ! reply
        failure(result).asInstanceOf[PipelineStages.RecoveryRequired].errorCode shouldBe "EQUIPMENT_REPLY_UNKNOWN"
        f.lots.expectNoMessage(100.millis)
        f.areas.expectNoMessage(100.millis)
      }
    }

    "accept a fresh actual CDSEM result and only then complete the lot and area" in {
      val f = new Fixture
      val simulator = spawn(new CdSemSimulator(CdSemConfig(waferIds = Seq("W1"),
        waferOutcomes = Map("W1" -> "PASS"))).apply(EquipmentConfig("CDSEM-01", "MET", 100.millis)))
      f.adapter.registerSimulator("CDSEM-01", simulator)
      val result = PipelineStages.measure(initial, f.context(withArea = true), "CDSEM-01")
      f.areas.expectMessageType[EquipmentAreaActor.StartProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Busy, "")
      f.lots.expectMessageType[RecordEquipmentJobStarted]
      f.lots.expectMessageType[RecordEquipmentJobCompleted].success shouldBe true
      f.areas.expectMessageType[EquipmentAreaActor.FinishProcess].replyTo.get !
        EquipmentAreaActor.AreaReply(true, EquipmentAreaActor.Finished, "")
      val next = Await.result(result, 3.seconds)
      next.ledgerSeq shouldBe initial.ledgerSeq + 1
      next.wafers("W1").cdValueHistory shouldBe List(32.0)
      testKit.stop(simulator)
    }
  }
}
