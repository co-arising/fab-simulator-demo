package net.imadz.fab.protocol

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import net.imadz.fab.protocol.ActorEquipmentAdapter.{JobBusy, OutcomeUnknown, SimulatorUnavailable}
import net.imadz.fab.simulation.{EquipmentConfig, GenericEquipmentSimulator}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicIntegerArray
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Uses only core dependencies and real actors, not the actor testkit. */
class ActorEquipmentAdapterSafetySpec extends AnyWordSpec with Matchers {

  private def withSystem[A](body: ActorSystem[Nothing] => A): A = {
    val config = ConfigFactory.parseString("""
      akka.actor.provider = local
      akka.loglevel = WARNING
    """).withFallback(ConfigFactory.load())
    val system = ActorSystem[Nothing](Behaviors.empty, s"adapter-safety-${UUID.randomUUID()}", config)
    try body(system)
    finally {
      system.terminate()
      Await.result(system.whenTerminated, 10.seconds)
    }
  }

  private def withWorkers[A](body: ExecutionContextExecutorService => A): A = {
    val workers = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
    try body(workers)
    finally {
      workers.shutdown()
      workers.awaitTermination(10, TimeUnit.SECONDS)
    }
  }

  private def simulator(system: ActorSystem[Nothing], equipmentId: String,
                        processingTime: FiniteDuration = 4.seconds,
                        faultProbability: Double = 0.0): ActorRef[SimulatorCommand] =
    system.systemActorOf(
      new GenericEquipmentSimulator().apply(EquipmentConfig(equipmentId, "test-area", processingTime, faultProbability)),
      s"simulator-${UUID.randomUUID()}"
    )

  "The real equipment simulator and adapter" should {
    "reply Busy to new commands on the same actor and still finish the original recipe on its timer" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      implicit val timeout: Timeout = Timeout(1.second)
      val equipmentId = "same-equipment"
      val ref = simulator(system, equipmentId)
      val adapter = new ActorEquipmentAdapter()
      adapter.registerSimulator(equipmentId, ref)

      Await.result(adapter.sendCommand(equipmentId, LoadFoup("original-foup", "port-1", Map.empty)), 1.second) shouldBe
        FoupArrived("original-foup", "port-1")
      val original = adapter.sendCommand(equipmentId, ProcessRecipe("original-recipe"))
      val busy = Await.result(adapter.queryStatus(equipmentId), 1.second)
      busy.status shouldBe Busy
      busy.currentJob should not be empty
      busy.portOccupancy shouldBe Map("port-1" -> Some("original-foup"))
      original.isCompleted shouldBe false

      // Raw protocol replies prove these commands are handled by the SAME busy
      // simulator, rather than a fake adapter or a different/idle actor.
      val rejected: Seq[EquipmentCommand] = Seq(
        ProcessRecipe("raw-duplicate"),
        LoadFoup("new-foup", "port-2", Map.empty),
        UnloadFoup("original-foup", "port-1"),
        TransferFoup("original-foup", "port-1", "port-2"),
        QueryStatus()
      )
      val busyReplies = rejected.map { cmd =>
        ref.ask[EquipmentEvent](replyTo => SimulateCommand(cmd, replyTo))
      }
      Await.result(Future.sequence(busyReplies), 1.second).foreach(reply => reply shouldBe busy)
      original.isCompleted shouldBe false

      val duplicate = Await.result(adapter.sendCommand(equipmentId, ProcessRecipe("adapter-duplicate")).failed, 1.second)
      duplicate shouldBe JobBusy(equipmentId, busy)
      original.isCompleted shouldBe false

      val completed = Await.result(original, 6.seconds)
      completed shouldBe a[JobCompleted]
      val job = completed.asInstanceOf[JobCompleted]
      Some(job.jobId) shouldBe busy.currentJob
      job.equipmentId shouldBe equipmentId
      job.result shouldBe LithoExposureResult(job.jobId, "original-recipe", Map.empty)
      Await.result(adapter.queryStatus(equipmentId), 1.second) shouldBe
        StatusReport(equipmentId, Idle, None, busy.portOccupancy)
    }

    "keep a real simulator fault as JobFailed rather than an admission or transport failure" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val equipmentId = "faulting-equipment"
      val adapter = new ActorEquipmentAdapter(3.seconds)
      adapter.registerSimulator(equipmentId, simulator(system, equipmentId, 100.millis, faultProbability = 1.0))
      val result = Await.result(adapter.sendCommand(equipmentId, ProcessRecipe("faulting-recipe")), 3.seconds)
      result shouldBe a[JobFailed]
      val failed = result.asInstanceOf[JobFailed]
      failed.equipmentId shouldBe equipmentId
      FaultType.all should contain(failed.errorCode)
    }
  }

  "ActorEquipmentAdapter failure classification" should {
    "fail missing registration publicly without synthesizing a device Error status" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val adapter = new ActorEquipmentAdapter()
      Await.result(adapter.queryStatus("missing").failed, 1.second) shouldBe SimulatorUnavailable("missing")
      Await.result(adapter.sendCommand("missing", ProcessRecipe("recipe")).failed, 1.second) shouldBe
        SimulatorUnavailable("missing")
    }

    "classify unexpected or wrong-device query replies as unknown and preserve actual status reports" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val equipmentId = "query-equipment"
      val adapter = new ActorEquipmentAdapter(3.seconds)
      val unexpected: Seq[EquipmentEvent] = Seq(
        JobFailed("unrelated-job", equipmentId, "HARDWARE_FAULT", "not a status reply"),
        FoupArrived("foup", "port"),
        StatusReport("another-equipment", Idle, None, Map.empty)
      )
      unexpected.foreach { event =>
        val ref = system.systemActorOf(Behaviors.receiveMessage[SimulatorCommand] {
          case SimulateCommand(QueryStatus(), replyTo) =>
            replyTo ! event
            Behaviors.same
          case _ => Behaviors.same
        }, s"bad-query-${UUID.randomUUID()}")
        adapter.registerSimulator(equipmentId, ref)
        val failure = Await.result(adapter.queryStatus(equipmentId).failed, 3.seconds)
        failure shouldBe a[OutcomeUnknown]
        val unknown = failure.asInstanceOf[OutcomeUnknown]
        unknown.equipmentId shouldBe equipmentId
        unknown.command shouldBe QueryStatus()
        unknown.getCause shouldBe a[IllegalStateException]
      }

      Seq(Idle, Error, Busy).foreach { status =>
        val report = StatusReport(equipmentId, status, None, Map.empty)
        val ref = system.systemActorOf(Behaviors.receiveMessage[SimulatorCommand] {
          case SimulateCommand(QueryStatus(), replyTo) =>
            replyTo ! report
            Behaviors.same
          case _ => Behaviors.same
        }, s"valid-query-${UUID.randomUUID()}")
        adapter.registerSimulator(equipmentId, ref)
        Await.result(adapter.queryStatus(equipmentId), 3.seconds) shouldBe report
      }
    }

    "keep an unexpected recipe acknowledgement unknown instead of claiming job completion or hardware failure" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val equipmentId = "unexpected-recipe"
      val adapter = new ActorEquipmentAdapter(3.seconds)
      val unexpected: Seq[EquipmentEvent] = Seq(
        StatusReport(equipmentId, Idle, None, Map.empty),
        StatusReport(equipmentId, Error, None, Map.empty),
        StatusReport("other-device", Busy, Some("other-job"), Map.empty),
        FoupArrived("foup", "port")
      )
      unexpected.foreach { event =>
        val ref = system.systemActorOf(Behaviors.receiveMessage[SimulatorCommand] {
          case SimulateCommand(_: ProcessRecipe, replyTo) =>
            replyTo ! event
            Behaviors.same
          case _ => Behaviors.same
        }, s"bad-recipe-${UUID.randomUUID()}")
        adapter.registerSimulator(equipmentId, ref)
        val cmd = ProcessRecipe("recipe")
        val failure = Await.result(adapter.sendCommand(equipmentId, cmd).failed, 3.seconds)
        failure shouldBe a[OutcomeUnknown]
        val unknown = failure.asInstanceOf[OutcomeUnknown]
        unknown.command shouldBe cmd
        unknown.getCause shouldBe a[IllegalStateException]
      }
    }

    "retain typed unknown outcomes and ask causes for both dispatched commands and status timeouts" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val equipmentId = "silent-equipment"
      val received = new LinkedBlockingQueue[EquipmentCommand]()
      val lateReply = Promise[ActorRef[EquipmentEvent]]()
      val ref = system.systemActorOf(Behaviors.receiveMessage[SimulatorCommand] {
        case SimulateCommand(cmd, replyTo) =>
          received.add(cmd)
          cmd match {
            case _: ProcessRecipe => lateReply.trySuccess(replyTo)
            case _ => ()
          }
          Behaviors.same
        case _ => Behaviors.same
      }, "silent-simulator")
      val adapter = new ActorEquipmentAdapter(200.millis)
      adapter.registerSimulator(equipmentId, ref)
      val cmd = ProcessRecipe("may-have-executed")
      val commandResult = adapter.sendCommand(equipmentId, cmd)
      val commandFailure = Await.result(commandResult.failed, 3.seconds)
      received.poll(1, TimeUnit.SECONDS) shouldBe cmd
      commandFailure shouldBe a[OutcomeUnknown]
      val commandUnknown = commandFailure.asInstanceOf[OutcomeUnknown]
      commandUnknown.equipmentId shouldBe equipmentId
      commandUnknown.command shouldBe cmd
      commandUnknown.getCause shouldBe a[TimeoutException]
      commandUnknown.getCause shouldBe commandUnknown.cause

      val statusFailure = Await.result(adapter.queryStatus(equipmentId).failed, 3.seconds)
      received.poll(1, TimeUnit.SECONDS) shouldBe QueryStatus()
      statusFailure shouldBe a[OutcomeUnknown]
      val statusUnknown = statusFailure.asInstanceOf[OutcomeUnknown]
      statusUnknown.command shouldBe QueryStatus()
      statusUnknown.getCause shouldBe a[TimeoutException]
      statusUnknown.getCause shouldBe statusUnknown.cause

      // A late success cannot change the already-failed Future or license a replay.
      Await.result(lateReply.future, 1.second) ! JobCompleted(
        "anonymous-job", equipmentId, LithoExposureResult("anonymous-job", cmd.recipeId, Map.empty)
      )
      Await.result(commandResult.failed, 1.second) shouldBe commandFailure
    }
  }

  "ActorEquipmentAdapter registry and subscribers" should {
    "retain every concurrent registration and subscription through public queries and notifications" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      withWorkers { workers =>
        val adapter = new ActorEquipmentAdapter(3.seconds)
        val entries = (0 until 128).map { index =>
          val equipmentId = s"parallel-$index"
          (equipmentId, simulator(system, equipmentId))
        }
        val delivered = new AtomicIntegerArray(entries.size)
        val ready = new CountDownLatch(8)
        val start = new CountDownLatch(1)
        val tasks = entries.zipWithIndex.map { case ((equipmentId, ref), index) =>
          Future {
            ready.countDown()
            start.await()
            adapter.registerSimulator(equipmentId, ref)
            adapter.subscribe("shared") { _ => delivered.incrementAndGet(index); () }
          }(workers)
        }
        try {
          ready.await(3, TimeUnit.SECONDS) shouldBe true
          start.countDown()
          Await.result(Future.sequence(tasks), 5.seconds)
          val reports = Await.result(Future.sequence(entries.map { case (id, _) => adapter.queryStatus(id) }), 5.seconds)
          reports.map(_.equipmentId) shouldBe entries.map(_._1)
          reports.foreach(report => report.status shouldBe Idle)

          val event = StatusReport("shared", Idle, None, Map.empty)
          adapter.notifySubscribers("shared", event)
          (0 until entries.size).foreach(index => delivered.get(index) shouldBe 1)
          adapter.unsubscribe("shared")
          adapter.notifySubscribers("shared", event)
          (0 until entries.size).foreach(index => delivered.get(index) shouldBe 1)
        } finally start.countDown()
      }
    }

    "isolate each throwing callback so every other subscriber still receives the event" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      val adapter = new ActorEquipmentAdapter()
      val received = new LinkedBlockingQueue[EquipmentEvent]()
      adapter.subscribe("subscribed")(event => { received.add(event); () })
      adapter.subscribe("subscribed")(event => { received.add(event); () })
      // Newest callback runs first and must not suppress the two older callbacks.
      adapter.subscribe("subscribed")(_ => throw new IllegalStateException("subscriber failure"))
      val event = StatusReport("subscribed", Idle, None, Map.empty)
      adapter.notifySubscribers("subscribed", event)
      received.asScala.toList shouldBe List(event, event)
    }

    "run callbacks outside the lock and preserve a snapshot across concurrent subscribe and unsubscribe" in withSystem { implicit system =>
      implicit val ec: ExecutionContext = system.executionContext
      withWorkers { workers =>
        val adapter = new ActorEquipmentAdapter()
        val entered = Promise[Unit]()
        val release = Promise[Unit]()
        val received = new LinkedBlockingQueue[String]()
        adapter.subscribe("subscribed")(_ => { received.add("original"); () })
        adapter.subscribe("subscribed") { _ =>
          entered.trySuccess(())
          Await.result(release.future, 10.seconds)
          received.add("blocking")
          ()
        }
        val event = StatusReport("subscribed", Idle, None, Map.empty)
        val notification = Future(adapter.notifySubscribers("subscribed", event))(workers)
        try {
          Await.result(entered.future, 3.seconds)
          // These MUST finish on another thread while the snapshot's callback is
          // blocked; same-thread reentrancy would not prove lock-free delivery.
          val mutations = Future {
            adapter.registerSimulator("another", system.ignoreRef[SimulatorCommand])
            adapter.subscribe("subscribed")(_ => { received.add("discarded"); () })
            adapter.unsubscribe("subscribed")
            adapter.subscribe("subscribed")(_ => { received.add("next-snapshot"); () })
          }(workers)
          Await.result(mutations, 3.seconds)
          notification.isCompleted shouldBe false
          release.trySuccess(())
          Await.result(notification, 3.seconds)
          received.asScala.toList shouldBe List("blocking", "original")
          adapter.notifySubscribers("subscribed", event)
          received.asScala.toList shouldBe List("blocking", "original", "next-snapshot")
        } finally {
          release.trySuccess(())
          Await.ready(notification, 3.seconds)
        }
      }
    }
  }
}
