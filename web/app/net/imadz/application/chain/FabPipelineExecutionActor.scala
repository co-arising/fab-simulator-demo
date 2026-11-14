package net.imadz.application.chain

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, Recovery}
import akka.persistence.typed.{PersistenceId, RecoveryCompleted => AkkaRecoveryCompleted}
import net.imadz.application.chain.FabExecutionModel.{FabDemoContext, FabDemoState}
import net.imadz.application.chain.FabExecutionModel.{StageError => FabStageError, StageFailedException => FabStageFailedException}
import net.imadz.application.chain.FabScenarioPipeline.PipelineStage
import net.imadz.domain.events.{FabSimulationEvent, GlobalStatusChanged, RecoveryEvent}
import net.imadz.monarch.{StaleRun, StageError => MonarchStageError, StageFailedException => MonarchStageFailedException}
import net.imadz.monarch.persistence.{DurableRunObserver, PersistentMonarch}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/**
 * ===== FabPipelineExecutionActor =====
 *
 * Sharded facade over `net.imadz.monarch.persistence.PersistentMonarch`.
 *
 * The hand-rolled `EventSourcedBehavior` that used to drive `FabPipelineProcessor`
 * directly (~500 lines: manual cursor journaling, `RecoveryCompleted` resume,
 * `PipelineRunRegistry` generations) is now split in two:
 *
 *   - **This actor (parent)** keeps the *domain* journal: `Started`,
 *     `StageStarted` / `StageCompleted` / `StageFailed` / `OcapHandled` /
 *     `StageProgress`, `AllCompleted` / `ExecutionFailed`, tagged `"fab-pipeline"`.
 *     The tag, the event schema, and the command protocol
 *     (`StartExecution` / `StopPipeline` / `ExecutionReply`) are unchanged, so
 *     `FabPipelineProjection`, `FabDemoController`'s timeline, and all
 *     `FabDemoService` call sites are untouched.
 *   - **A `PersistentMonarch` child** (`"run"`, journal
 *     `FabPipelineExecutionRun|<workOrderId>`) owns the *mechanics*: per-stage
 *     `StageDone` journaling with protobuf state snapshots, cursor-based resume
 *     on `RecoveryCompleted`, and `RunRegistry` generation guards. The parent
 *     translates child lifecycle callbacks into domain events.
 *
 * == OCAP placement ==
 *
 * OCAP interception lives *inside the stage interpreter*, not in
 * `PersistentMonarch`'s `failurePolicy`, to preserve Fab's structured failure
 * notification before OCAP and its distinct `OcapHandled` domain event.
 * Monarch 0.2.1 also checkpoints failure-policy resolutions; the old 0.2 cursor
 * gap is no longer the reason for this placement. Inline OCAP returns a marked
 * state and checkpoints `StageDone`; only `onCheckpointCommitted` forwards it
 * to the parent (P2: the domain journal never claims a failed stage succeeded).
 *
 * == Crash recovery ==
 *
 * The parent first reads the child's committed journal and persists any missing
 * domain progress or terminal fact. It spawns a recovering child only afterwards;
 * a recorded child terminal does not need another execution actor. The durable
 * initial input and finite plan cover a parent-start/child-not-started window.
 * Missing legacy input/plan, conflicting history and unknown acknowledgements
 * require controlled recovery rather than an empty-input restart.
 *
 * Child callbacks carry an incarnation checked by the receiving parent, including
 * same-reference supervised restarts. The child lease and PipelineRunRegistry
 * guard future execution, but cannot cancel already-issued equipment I/O. Journal
 * visibility, external job reattachment and cross-JVM ownership remain separate
 * host obligations; querying history alone does not prove an unknown write settled.
 *
 * == Deterministic Saga Re-attach ==
 *
 * Saga transactions use [[net.imadz.application.services.SagaIdGenerator.generate]]
 * to produce deterministic UUID v3 transaction IDs, enabling sagas to be idempotently
 * re-attached after recovery. This lives inside the stage bodies
 * (`FabScenarioPipeline.runStage`), which the interpreter calls unchanged.
 */
object FabPipelineExecutionActor {

  val EntityKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("FabPipelineExecution")

  /** Journal tag for [[eventsByTag]] Projection subscriptions. */
  val Tag = "fab-pipeline"

  /**
   * How long the step interpreter waits for lot notifications to be confirmed
   * before failing the step (2026-10-18 arbitration: lot notifications are
   * acknowledged). Local lot actors confirm in milliseconds; 5s is generous.
   */
  val LotAckTimeout: akka.util.Timeout =
    akka.util.Timeout(5, java.util.concurrent.TimeUnit.SECONDS)

  /** PersistenceId type for the PersistentMonarch child (one per parent entity). */
  private val ChildPersistenceId = "FabPipelineExecutionRun"

  /** Child actor name under the sharded parent. */
  private val ChildActorName = "run"

  // ============================================================
  // Protocol (external — unchanged)
  // ============================================================

  sealed trait Command
  sealed trait Event
  sealed trait State
  sealed trait ExecutionReply

  /** Start pipeline execution with the given stages and context. */
  final case class StartExecution(
      scenarioId: String,
      workOrderId: String,
      initialState: FabDemoState,
      stages: Seq[PipelineStage],
      ctx: FabDemoContext,
      replyTo: ActorRef[ExecutionReply]
  ) extends Command

  final case class GetExecutionState(replyTo: ActorRef[State]) extends Command

  /** M3.5: Stop/crash the pipeline actor. Causes Behavior.stopped → sharding restarts → RecoveryCompleted. */
  final case class StopPipeline(workOrderId: String) extends Command

  /** Internal: intra-stage progress notification (replaces ctx.publish(GlobalStatusChanged)). */
  final case class StageProgressEvent(status: String, detail: String, phase: String) extends Command

  // ---- Child engine → parent: internal lifecycle callbacks ----

  /** Internal: the child engine started a stage. */
  private final case class EngineStageStarted(cursor: String) extends Command

  /** Internal: a stage failed (structured); journaled BEFORE OCAP runs,
    * mirroring the old `onStageFailed` hook ordering. Sent by the interpreter
    * side-channel because `RunObserver.onStageFailed` only carries a detail string. */
  private final case class EngineStageFailed(cursor: String, error: FabStageError) extends Command

  /** Internal: the child engine completed a stage (cleanly, or OCAP-resolved —
    * see `runState.lastOutcome`). */
  private final case class EngineStageCompleted(cursor: String, runState: FabRunState) extends Command

  /** Internal: the child is recovering; `completedCount` stages already done. */
  private final case class EngineRecovering(completedCount: Int) extends Command

  /** Internal: the child run drained the whole queue. */
  private final case class EngineRunSucceeded() extends Command

  /** Internal: the child run failed (OCAP itself threw, or an unexpected error). */
  private final case class EngineRunFailed(cursor: String, reason: String) extends Command

  private final case class HistoryLoaded(owner: java.util.UUID,
                                         result: scala.util.Try[Option[FabRunHistory.History]]) extends Command
  private final case class ChildCallback(owner: java.util.UUID, command: Command) extends Command
  private case object ReconcileRun extends Command
  private case object ChildStopped extends Command

  /** Unknown lot outcomes require reconciliation, not business failure or OCAP. */
  private final case class EngineConfirmationUnknown(cursor: String, error: FabStageError) extends Command

  /** Internal: the child's `StartRun` reply, forwarded to the original `replyTo`. */
  private final case class EngineStartReplied(replyTo: ActorRef[ExecutionReply], reply: ExecutionReply) extends Command

  // ---- Events (unchanged schema — projection & timeline read these) ----

  final case class StageProgress(
      status: String,
      detail: String,
      phase: String,
      timestamp: Long
  ) extends Event

  final case class Started(
      scenarioId: String,
      workOrderId: String,
      stageCount: Int
  ) extends Event

  final case class StartedWithInput(scenarioId: String, workOrderId: String,
                                    initialState: FabDemoState, plan: Vector[String],
                                    planBytes: Vector[Byte]) extends Event

  final case class StageStarted(
      phase: String,
      timestamp: Long
  ) extends Event

  final case class StageCompleted(
      phase: String,
      timestamp: Long,
      metadata: Map[String, String],
      fabState: Option[FabDemoState] = None
  ) extends Event

  final case class StageFailed(
      phase: String,
      error: FabExecutionModel.StageError,
      timestamp: Long
  ) extends Event

  /** P2: OCAP handling of a failed stage — advances the cursor without claiming success. */
  final case class OcapHandled(
      phase: String,
      error: FabExecutionModel.StageError,
      timestamp: Long,
      fabState: FabDemoState
  ) extends Event

  final case class AllCompleted(
      scenarioId: String,
      workOrderId: String,
      totalWafers: Int = 0,
      passedWafers: Int = 0,
      reworkedWafers: Int = 0,
      scrappedWafers: Int = 0
  ) extends Event

  final case class RecoveryRequired(phase: String, error: FabStageError, timestamp: Long) extends Event

  final case class ExecutionFailed(
      phase: String,
      reason: String
  ) extends Event

  // ---- States (unchanged) ----

  case object Idle extends State

  final case class Executing(
      scenarioId: String,
      workOrderId: String,
      completedPhases: List[String],
      stageCount: Int = 0,
      fabDemoState: Option[FabDemoState] = None
  ) extends State {
    def lastCompletedPhase: Option[String] = completedPhases.lastOption
    def completedCount: Int = completedPhases.size
  }

  final case class Completed(
      scenarioId: String,
      workOrderId: String
  ) extends State

  final case class RestoredExecution(execution: Executing, planBytes: Vector[Byte]) extends State

  final case class AwaitingRecovery(execution: Executing, phase: String, error: FabStageError,
                                    planBytes: Vector[Byte] = Vector.empty) extends State

  final case class Failed(
      phase: String,
      reason: String
  ) extends State

  // ---- Replies (unchanged) ----

  case object Accepted extends ExecutionReply
  final case class Rejected(reason: String) extends ExecutionReply

  // ============================================================
  // Child run host: interpreter + observer for one PersistentMonarch child
  // ============================================================

  /**
   * Builds the stage interpreter and run observer for a `PersistentMonarch`
   * child. One host per child incarnation.
   *
   * @param workOrderId    entity/run id
   * @param freshCtx       the `FabDemoContext` from `StartExecution` (fresh start),
   *                       or `None` on recovery (rebuilt via `contextFactory`)
   * @param contextFactory rebuilds `FabDemoContext` from `(scenarioId, workOrderId)`
   * @param parentSelf     the sharded parent, for journal-bound callbacks
   * @param publisher      demo UX event sink (recovery affordance)
   */
  private class ChildRunHost(
      workOrderId: String,
      freshCtx: Option[FabDemoContext],
      contextFactory: (String, String) => FabDemoContext,
      parentSelf: ActorRef[Command],
      owner: java.util.UUID,
      publisher: FabSimulationEvent => Unit
  )(implicit ec: ExecutionContext, scheduler: Scheduler) {

    private def notifyParent(command: Command): Unit = parentSelf ! ChildCallback(owner, command)

    @volatile private var cachedCtx: FabDemoContext = _

    /**
     * Lazily built once per child incarnation, on first interpreter invocation.
     * Registers a fresh [[PipelineRunRegistry]] generation (fresh start and
     * recovery alike) and guards `stageProgressFn` so intra-stage progress
     * flows through the parent's journal as `StageProgress` events.
     */
    private def ctxFor(runState: FabRunState): FabDemoContext = {
      var c = cachedCtx
      if (c == null) {
        val generation = PipelineRunRegistry.register(workOrderId)
        val base = freshCtx.getOrElse(contextFactory(runState.scenarioId, workOrderId))
        val guarded = base.copy(runToken = () => PipelineRunRegistry.isFresh(workOrderId, generation))
        // P0 (callback guards): superseded runs must not journal or publish.
        guarded.stageProgressFn = (status, detail, phase) =>
          if (guarded.runToken()) notifyParent(StageProgressEvent(status, detail, phase))
        cachedCtx = guarded
        c = guarded
      }
      c
    }

    private def toFabError(cursor: String, ex: Throwable): FabStageError = ex match {
      case MonarchStageFailedException(merr) =>
        FabStageError(merr.stage, merr.code, merr.errorCode, merr.detail)
      case FabStageFailedException(error) => error
      case error: PipelineStages.RecoveryRequired =>
        FabStageError(cursor, None, error.errorCode, error.detail)
      case error: net.imadz.fab.protocol.ActorEquipmentAdapter.OutcomeUnknown =>
        FabStageError(cursor, Some(error.equipmentId), "EQUIPMENT_OUTCOME_UNKNOWN", error.getMessage)
      case error: net.imadz.fab.protocol.ActorEquipmentAdapter.JobBusy =>
        FabStageError(cursor, Some(error.equipmentId), "EQUIPMENT_JOB_BUSY", error.getMessage)
      case error: net.imadz.fab.protocol.ActorEquipmentAdapter.SimulatorUnavailable =>
        FabStageError(cursor, Some(error.equipmentId), "EQUIPMENT_UNAVAILABLE", error.getMessage)
      case _ =>
        FabStageError(cursor, None, "UNEXPECTED", Option(ex.getMessage).getOrElse(ex.toString))
    }

    private def requiresReconciliation(ex: Throwable, error: FabStageError): Boolean =
      ex.isInstanceOf[PipelineStages.RecoveryRequired] ||
        ex.isInstanceOf[net.imadz.fab.protocol.ActorEquipmentAdapter.OutcomeUnknown] ||
        ex.isInstanceOf[net.imadz.fab.protocol.ActorEquipmentAdapter.JobBusy] ||
        ex.isInstanceOf[net.imadz.fab.protocol.ActorEquipmentAdapter.SimulatorUnavailable] ||
        Set("LOT_ACK_TIMEOUT", "LOT_ACK_UNKNOWN").contains(error.errorCode)

    /**
     * Stage interpreter: `FabScenarioPipeline.runStage` with OCAP interception
     * inline.
     *
     * Cursor naming matches `PersistentMonarch` exactly (`stageName#index` over
     * `stages.zipWithIndex`): `completedCount` always equals the number of
     * journaled `StageDone`, so `stageName#completedCount` is the current
     * stage's cursor on fresh runs and on recovery alike.
     *
     * Failure path: any stage failure (classified `StageFailedException` or
     * unexpected) is reported to the parent as `EngineStageFailed` *before*
     * `invokeOcapInterceptor` runs — the same order the old `onStageFailed`
     * hook produced. If OCAP recovers, the returned state is marked
     * `OcapResolved` so the parent journals `OcapHandled` (cursor advances,
     * no success claimed). If OCAP itself throws, the failure propagates and
     * the child journals `RunFailedEvt`.
     */
    def interpreter(stage: PipelineStage, runState: FabRunState): Future[FabRunState] = {
      val fctx = ctxFor(runState)
      // P0 (staleness): a superseded run dies here at the next stage boundary
      // instead of running free with real side effects.
      if (!fctx.runToken()) return Future.failed(StaleRun)
      val cursor = s"${FabPipelineProcessor.stageName(stage)}#${runState.completedCount}"
      if (fctx.runToken()) notifyParent(EngineStageStarted(cursor))

      // 2026-10-18 (arbitrated): lot notifications are acknowledged. Install a
      // fresh tracking gate for this step; the step only counts as done once
      // every lot notification it sent is confirmed. A lot that never confirms
      // fails the step (LOT_ACK_TIMEOUT) instead of diverging silently.
      val ackGate = LotAckGate.tracking(LotAckTimeout)
      fctx.lotAckGate = ackGate

      def attempt[A](body: => Future[A]): Future[A] =
        try body catch { case NonFatal(ex) => Future.failed(ex) }

      def drain(gate: LotAckGate): Future[Unit] = gate.seal().recoverWith {
        case LotAckGate.LotRejected(detail) =>
          Future.failed(FabStageFailedException(FabStageError(cursor, None, "LOT_ACK_REJECTED", detail)))
        case NonFatal(ex) =>
          val code = if (ex.isInstanceOf[java.util.concurrent.TimeoutException]) "LOT_ACK_TIMEOUT" else "LOT_ACK_UNKNOWN"
          Future.failed(FabStageFailedException(FabStageError(cursor, None, code,
            s"lot notification outcome is unknown: ${Option(ex.getMessage).getOrElse(ex.toString)}")))
      }

      // Even a failed body can already have sent lot commands. Settle them before
      // deciding whether business recovery is safe; an unknown outcome wins.
      def acknowledged[A](body: => Future[A], gate: LotAckGate): Future[A] =
        attempt(body).transformWith { result =>
          drain(gate).transformWith { confirmation =>
            if (!fctx.runToken()) Future.failed(StaleRun)
            else result match {
              // Equipment/area uncertainty is not made safe by a different lot
              // request's explicit rejection. Both channels must be reconciled.
              case scala.util.Failure(ex) if requiresReconciliation(ex, toFabError(cursor, ex)) => Future.failed(ex)
              case _ => confirmation match {
                case scala.util.Failure(ex) => Future.failed(ex)
                case scala.util.Success(_) => Future.fromTry(result)
              }
            }
          }
        }

      acknowledged(FabScenarioPipeline.runStage(stage, runState.state, fctx), ackGate)
        .map(next => runState.copy(state = next, completedCount = runState.completedCount + 1, lastOutcome = None))
        .recoverWith {
          case NonFatal(ex) if ex ne StaleRun =>
            val fabErr = toFabError(cursor, ex)
            if (!fctx.runToken()) Future.failed(StaleRun)
            else {
              // A timeout is not a rejection. Do not run OCAP side effects until
              // the host has confirmed what happened to the outstanding writes.
              if (requiresReconciliation(ex, fabErr)) {
                notifyParent(EngineConfirmationUnknown(cursor, fabErr))
                Future.failed(StaleRun)
              } else {
                notifyParent(EngineStageFailed(cursor, fabErr))
                if (fabErr.errorCode == "LOT_ACK_REJECTED") Future.failed(MonarchStageFailedException(
                  MonarchStageError(fabErr.stageName, fabErr.equipId, fabErr.errorCode, fabErr.detail)))
                else {
                val recoveryGate = LotAckGate.tracking(LotAckTimeout)
                fctx.lotAckGate = recoveryGate
                acknowledged(FabScenarioPipeline.invokeOcapInterceptor(runState.state, fctx, fabErr), recoveryGate)
                  .recoverWith {
                    case NonFatal(ex) if (ex ne StaleRun) && requiresReconciliation(ex, toFabError(cursor, ex)) =>
                      notifyParent(EngineConfirmationUnknown(cursor, toFabError(cursor, ex)))
                      Future.failed(StaleRun)
                    case FabStageFailedException(error) =>
                      Future.failed(MonarchStageFailedException(MonarchStageError(
                        error.stageName, error.equipId, error.errorCode, error.detail)))
                  }
                  .map(recovered => runState.copy(state = recovered,
                    completedCount = runState.completedCount + 1,
                    lastOutcome = Some(FabRunState.OcapResolved(fabErr))))
                }
              }
            }
        }
    }

    /** Child lifecycle → parent journal commands. Completion is forwarded only
      * after the child's checkpoint commits, never from speculative progress.
      * `onStageFailed` is not forwarded: the interpreter side-channel already
      * sent the structured failure before OCAP ran. */
    def observer: DurableRunObserver[FabRunState] = new DurableRunObserver[FabRunState] {
      override def onCheckpointCommitted(cursor: String, metadata: Map[String, String], snapshot: FabRunState): Unit =
        notifyParent(EngineStageCompleted(cursor, snapshot))
      override def onRecovery(runId: String, completedCount: Int): Unit =
        notifyParent(EngineRecovering(completedCount))
      override def onCompleted(runId: String): Unit =
        notifyParent(EngineRunSucceeded())
      override def onFailed(runId: String, cursor: String, reason: String): Unit =
        notifyParent(EngineRunFailed(cursor, reason))
    }

    def childBehavior(
        entityId: String,
        stages: Seq[PipelineStage]
    ): Behavior[PersistentMonarch.Command[FabRunState]] =
      PersistentMonarch.behavior[PipelineStage, FabRunState](
        persistenceId = PersistenceId(ChildPersistenceId, entityId),
        stages = stages,
        interpreter = interpreter,
        stateCodec = FabRunStateCodec.protobuf,
        stageName = FabPipelineProcessor.stageName,
        failurePolicy = None, // OCAP interception is inline in the interpreter (see above)
        observer = observer
      )
  }

  // ============================================================
  // Factory
  // ============================================================

  /**
   * Create the parent facade behavior for a FabPipelineExecutionActor.
   *
   * @param entityId          unique entity ID (e.g. workOrderId)
   * @param contextFactory    reconstructs FabDemoContext from (scenarioId, workOrderId) on recovery
   * @param stateFactory      reconstructs FabDemoState from workOrderId on recovery
   *                          (fallback when the domain journal has no snapshot yet)
   * @param stageResolver     resolves scenarioId → Seq[PipelineStage] for recovery
   */
  def apply(
      entityId: String,
      contextFactory: (String, String) => FabDemoContext,
      stateFactory: String => FabDemoState,
      stageResolver: String => Seq[PipelineStage],
      publisher: FabSimulationEvent => Unit = _ => ()
  )(implicit ec: ExecutionContext): Behavior[Command] = {
    Behaviors.setup { ctx =>
      val persistenceId = PersistenceId(EntityKey.name, entityId)
      val startTime = System.currentTimeMillis()

      def publishSafely(event: FabSimulationEvent): Unit =
        try publisher(event) catch { case NonFatal(ex) => ctx.log.warn("Fab UX observer failed", ex) }

      /** Set while the child is recovering, until the first post-recovery
        * stage completion (or run end) — drives the RECOVERED UX event. */
      var awaitingRecovered = false
      var reconciling = false
      var activeChildOwner: Option[java.util.UUID] = None
      val historyOwner = java.util.UUID.randomUUID()
      implicit val materializer: akka.stream.Materializer = akka.stream.Materializer(ctx.system)

      def frontierFailure(execution: Executing, detail: String): Effect[Event, State] =
        Effect.persist(RecoveryRequired("frontier", FabStageError("frontier", None,
          "FRONTIER_CONFLICT", detail), System.currentTimeMillis())).thenRun { _ =>
          activeChildOwner = None
          PipelineRunRegistry.register(execution.workOrderId)
          ctx.child(ChildActorName).foreach(ctx.stop)
        }

      def recordedPlan(state: State): Option[Vector[String]] = state match {
        case RestoredExecution(_, encoded) => FabPipelinePlanCodec.decode(encoded.toArray).toOption.map { stages =>
          stages.zipWithIndex.map { case (stage, index) => s"${FabPipelineProcessor.stageName(stage)}#$index" }
        }
        case _ => None
      }

      def recoveryFailure(execution: Executing, detail: String): Effect[Event, State] =
        Effect.persist(RecoveryRequired("recovery", FabStageError("recovery", None,
          "HISTORY_UNAVAILABLE", detail), System.currentTimeMillis()))

      def spawnChild(
          scenarioId: String,
          workOrderId: String,
          stages: Seq[PipelineStage],
          freshCtx: Option[FabDemoContext]
      ): ActorRef[PersistentMonarch.Command[FabRunState]] = {
        val owner = java.util.UUID.randomUUID()
        activeChildOwner = Some(owner)
        val host = new ChildRunHost(workOrderId, freshCtx, contextFactory, ctx.self, owner, publisher)(
          ctx.executionContext, ctx.system.scheduler)
        val child = ctx.spawn(host.childBehavior(entityId, stages), ChildActorName)
        ctx.watchWith(child, ChildCallback(owner, ChildStopped))
        child
      }

      def publishRecovering(workOrderId: String, completedCount: Int): Unit = {
        // RECOVERING/RECOVERED RecoveryEvent + GlobalStatusChanged are demo UX affordance.
        // Published directly via publisher (not journal) — production recovery is transparent.
        publishSafely(GlobalStatusChanged("RECOVERING", s"Crash recovery for $workOrderId", "Recovery"))
        publishSafely(RecoveryEvent(
          workOrderId, "RECOVERING",
          eventsReplayed = completedCount,
          phasesSkipped = completedCount,
          recoveryTimeMs = System.currentTimeMillis() - startTime,
          detail = s"Recovering: $completedCount phases completed"))
        awaitingRecovered = true
      }

      def publishRecovered(workOrderId: String, completedCount: Int): Unit = {
        publishSafely(RecoveryEvent(
          workOrderId, "RECOVERED",
          eventsReplayed = completedCount,
          phasesSkipped = completedCount,
          recoveryTimeMs = System.currentTimeMillis() - startTime,
          detail = s"Recovery succeeded: $completedCount phases skipped, resuming pipeline"))
        awaitingRecovered = false
      }

      val handleCommand: (State, Command) => Effect[Event, State] = { (state, cmd) =>
        val executionState = state match {
          case RestoredExecution(execution, _) => execution
          case other => other
        }
        (executionState, cmd) match {
          case (_, GetExecutionState(replyTo)) =>
            replyTo ! executionState
            Effect.none

          case (e: Executing, ReconcileRun) if !reconciling =>
            if (ctx.child(ChildActorName).nonEmpty) recoveryFailure(e, "Previous run actor is still present")
            else {
              reconciling = true
              publishRecovering(e.workOrderId, e.completedCount)
              val query = try FabRunHistory.read(ctx.system, PersistenceId(ChildPersistenceId, entityId).id)
                catch { case NonFatal(ex) => Future.failed(ex) }
              ctx.pipeToSelf(query)(result => HistoryLoaded(historyOwner, result))
              Effect.none
            }

          case (e: Executing, HistoryLoaded(owner, result)) if reconciling && owner == historyOwner =>
            reconciling = false
            val bytes = state match {
              case RestoredExecution(_, encoded) => Some(encoded.toArray)
              case _ => None
            }
            val prepared = try {
              val history = result.get
              val stages = bytes match {
                case Some(encoded) => FabPipelinePlanCodec.decode(encoded).fold(
                  problem => throw new IllegalArgumentException(problem), identity)
                case None if history.exists(_.terminal.nonEmpty) => Vector.empty[PipelineStage]
                case None => throw new IllegalArgumentException("Legacy run has no durable plan; manual recovery required")
              }
              history.foreach { h =>
                require(h.runId == e.workOrderId && h.initial.scenarioId == e.scenarioId, "Run identity mismatch")
                require(e.completedPhases == h.checkpoints.take(e.completedCount).map(_.cursor).toList,
                  "Parent progress is not a prefix of committed run history")
                if (e.completedCount > 0) require(e.fabDemoState.contains(h.checkpoints(e.completedCount - 1).state.state),
                  "Parent snapshot conflicts with committed run history")
                if (bytes.nonEmpty) require(h.plan == stages.zipWithIndex.map { case (stage, index) =>
                  s"${FabPipelineProcessor.stageName(stage)}#$index"
                }, "Recorded plan differs from committed run plan")
              }
              require(history.nonEmpty || e.fabDemoState.nonEmpty, "No durable initial input")
              Right((history, stages))
            } catch { case NonFatal(ex) => Left(Option(ex.getMessage).getOrElse(ex.toString)) }
            prepared match {
              case Left(problem) => recoveryFailure(e, problem)
              case Right((history, stages)) =>
                val progress: Vector[Event] = history.toVector.flatMap(_.checkpoints.drop(e.completedCount).map { checkpoint =>
                  checkpoint.error match {
                    case Some(error) => OcapHandled(checkpoint.cursor, error, System.currentTimeMillis(), checkpoint.state.state)
                    case None => StageCompleted(checkpoint.cursor, System.currentTimeMillis(), Map.empty,
                      Some(checkpoint.state.state))
                  }
                })
                val terminal: Vector[Event] = history.toVector.flatMap(h => h.terminal.toVector.map {
                  case FabRunHistory.Completed =>
                    val wafers = h.latest.state.wafers.values.toVector
                    AllCompleted(e.scenarioId, e.workOrderId, wafers.size,
                      wafers.count(_.classification.contains("PASS")), wafers.count(_.reworkCount > 0),
                      wafers.count(_.classification.contains("SCRAP")))
                  case FabRunHistory.Failed(cursor, reason) => ExecutionFailed(cursor, reason)
                })
                val events = progress ++ terminal
                val effect = if (events.isEmpty) Effect.none[Event, State] else Effect.persist[Event, State](events)
                effect.thenRun { _ =>
                  if (terminal.nonEmpty) publishRecovered(e.workOrderId, history.get.checkpoints.size)
                  else {
                    val child = spawnChild(e.scenarioId, e.workOrderId, stages, None)
                    if (history.isEmpty) {
                      val owner = activeChildOwner.get
                      val replies = ctx.messageAdapter[PersistentMonarch.RunReply] {
                        case PersistentMonarch.Accepted(_) => ChildCallback(owner, EngineRecovering(0))
                        case PersistentMonarch.Rejected(_, why) => ChildCallback(owner, EngineConfirmationUnknown("start",
                          FabStageError("start", None, "START_REJECTED", why)))
                      }
                      child ! PersistentMonarch.StartRun(e.workOrderId,
                        FabRunState.initial(e.scenarioId, e.workOrderId, e.fabDemoState.get), replies)
                    }
                  }
                }
            }

          // ---- Start execution ----
          case (Idle, StartExecution(scenarioId, workOrderId, initialState, stages, fctx0, replyTo)) =>
            FabPipelinePlanCodec.encode(stages) match {
              case Left(problem) => replyTo ! Rejected(problem); Effect.none
              case Right(bytes) =>
                val plan = stages.zipWithIndex.map { case (stage, index) =>
                  s"${FabPipelineProcessor.stageName(stage)}#$index"
                }.toVector
                Effect.persist(StartedWithInput(scenarioId, workOrderId, initialState, plan, bytes.toVector))
                  .thenRun { _ =>
                    val child = spawnChild(scenarioId, workOrderId, stages, Some(fctx0))
                    val owner = activeChildOwner.get
                    val replyAdapter: ActorRef[PersistentMonarch.RunReply] =
                      ctx.messageAdapter[PersistentMonarch.RunReply] {
                        case PersistentMonarch.Accepted(_) => ChildCallback(owner, EngineStartReplied(replyTo, Accepted))
                        case PersistentMonarch.Rejected(_, why) => ChildCallback(owner, EngineStartReplied(replyTo, Rejected(why)))
                      }
                    child ! PersistentMonarch.StartRun[FabRunState](
                      workOrderId, FabRunState.initial(scenarioId, workOrderId, initialState), replyAdapter)
                  }
            }

          // ---- Child's StartRun reply → original replyTo ----
          case (_, EngineStartReplied(replyTo, reply)) =>
            replyTo ! reply
            Effect.none

          // ---- Child engine: stage started ----
          case (_: Executing, EngineStageStarted(cursor)) =>
            ctx.log.info(s"[M3.5] >>> STAGE START: $cursor")
            Effect.persist(StageStarted(cursor, System.currentTimeMillis()))

          // ---- Child engine: stage failed (before OCAP) ----
          case (_: Executing, EngineStageFailed(cursor, error)) =>
            ctx.log.warn(s"[M3.5] Stage FAILED: $cursor — ${error.errorCode}: ${error.detail}")
            Effect.persist(StageFailed(cursor, error, System.currentTimeMillis()))

          case (e: Executing, ChildStopped) =>
            val error = FabStageError(e.lastCompletedPhase.getOrElse("run"), None,
              "CHILD_STOPPED", "Run actor stopped; its journal must be reconciled before execution can resume")
            Effect.persist(RecoveryRequired(error.stageName, error, System.currentTimeMillis())).thenRun { _ =>
              activeChildOwner = None
              PipelineRunRegistry.register(e.workOrderId)
            }

          case (e: Executing, EngineConfirmationUnknown(cursor, error)) =>
            ctx.log.error("[M3.5] Confirmation unknown at {}: {}; controlled recovery required", cursor, error.detail)
            Effect.persist(RecoveryRequired(cursor, error, System.currentTimeMillis())).thenRun { _ =>
              activeChildOwner = None
              PipelineRunRegistry.register(e.workOrderId)
              ctx.child(ChildActorName).foreach(ctx.stop)
            }

          // ---- Child engine: stage completed (clean or OCAP-resolved) ----
          case (e: Executing, EngineStageCompleted(cursor, runState)) if e.completedPhases.contains(cursor) =>
            if (e.lastCompletedPhase.contains(cursor) && runState.workOrderId == e.workOrderId &&
                runState.scenarioId == e.scenarioId && runState.completedCount == e.completedCount &&
                e.fabDemoState.contains(runState.state)) Effect.none
            else frontierFailure(e, s"Conflicting or out-of-order duplicate checkpoint $cursor")

          case (e: Executing, EngineStageCompleted(cursor, runState)) if
              runState.workOrderId != e.workOrderId || runState.scenarioId != e.scenarioId ||
              runState.completedCount != e.completedCount + 1 ||
              !recordedPlan(state).exists(_.lift(e.completedCount).contains(cursor)) =>
            frontierFailure(e, s"Checkpoint $cursor is not the next recorded stage for this run")

          case (e: Executing, EngineStageCompleted(cursor, runState)) =>
            ctx.log.info(s"[M3.5] <<< STAGE DONE: $cursor")
            val effect = runState.lastOutcome match {
              case Some(FabRunState.OcapResolved(err)) =>
                // StageFailed was already journaled via EngineStageFailed; P2: the
                // cursor advances without claiming success.
                Effect.persist[Event, State](OcapHandled(cursor, err, System.currentTimeMillis(), runState.state))
              case None =>
                Effect.persist[Event, State](StageCompleted(cursor, System.currentTimeMillis(), Map.empty, Some(runState.state)))
            }
            effect.thenRun { _ =>
              if (awaitingRecovered) publishRecovered(e.workOrderId, e.completedPhases.size)
            }

          // ---- Child engine: recovering ----
          case (e: Executing, EngineRecovering(completedCount)) =>
            publishRecovering(e.workOrderId, completedCount)
            Effect.none

          // ---- Child engine: run fully done ----
          case (e: Executing, EngineRunSucceeded()) if e.completedCount != e.stageCount ||
              !recordedPlan(state).contains(e.completedPhases.toVector) =>
            frontierFailure(e, "Run success arrived before its complete committed stage prefix")

          case (e: Executing, EngineRunSucceeded()) =>
            val wafers = e.fabDemoState.map(_.wafers.values.toSeq).getOrElse(Seq.empty)
            val passCount = wafers.count(w => w.classification.contains("PASS"))
            val scrapCount = wafers.count(w => w.classification.contains("SCRAP"))
            val reworkCount = wafers.count(_.reworkCount > 0)
            Effect.persist(AllCompleted(e.scenarioId, e.workOrderId,
              totalWafers = wafers.size, passedWafers = passCount,
              reworkedWafers = reworkCount, scrappedWafers = scrapCount)).thenRun { _ =>
              if (awaitingRecovered) publishRecovered(e.workOrderId, e.completedPhases.size)
            }

          // ---- Child engine: run failed ----
          case (_: Executing, EngineRunFailed(cursor, reason)) =>
            ctx.log.error(s"[M3.5] PIPELINE FAILED at $cursor: $reason")
            Effect.persist(ExecutionFailed(cursor, reason))

          // ---- Intra-stage progress (replaces ctx.publish(GlobalStatusChanged)) ----
          case (_: Executing, StageProgressEvent(status, detail, phase)) =>
            Effect.persist(StageProgress(status, detail, phase, System.currentTimeMillis()))

          // ---- M3.5: Stop/crash the pipeline ----
          case (es: Executing, StopPipeline(woId)) =>
            Effect.none.thenRun { _ =>
              // Bump the generation NOW (not at recovery): the pre-crash Future chain dies at its
              // next stage boundary instead of running free — with real side effects — for the
              // whole sharding restart-backoff window (~10s). The interpreter checks the
              // PipelineRunRegistry token on every stage entry; the respawned child also
              // invalidates the old engine via its own RunRegistry generation.
              PipelineRunRegistry.register(woId)
              publishSafely(RecoveryEvent(woId, "CRASH_DETECTED",
                eventsReplayed = es.completedPhases.size, phasesSkipped = es.completedCount,
                recoveryTimeMs = System.currentTimeMillis(),
                detail = s"Actor crash for workOrder $woId (${es.completedCount} phases completed)"))
              ctx.log.warn(s"[FabPipelineExecutionActor:$entityId] Crash injected, stopping actor")
              throw new RuntimeException(s"Pipeline crash injected for workOrder $woId")
            }

          case (Idle, StopPipeline(woId)) =>
            Effect.none.thenRun { _ =>
              PipelineRunRegistry.register(woId)
              publishSafely(RecoveryEvent(woId, "CRASH_DETECTED", 0, 0, System.currentTimeMillis(),
                s"Actor crash injected (idle state)"))
              ctx.log.warn(s"[FabPipelineExecutionActor:$entityId] Crash injected in Idle state")
              throw new RuntimeException(s"Crash injected for actor $entityId")
            }

          // ---- Ignore late engine callbacks after completion ----
          case (_: Completed, _: EngineStageStarted)   => Effect.none
          case (_: Completed, _: EngineStageFailed)    => Effect.none
          case (_: Completed, _: EngineStageCompleted) => Effect.none
          case (_: Completed, _: EngineRunSucceeded)   => Effect.none
          case (_: Completed, _: EngineRunFailed)      => Effect.none
          case (_: Failed, _: EngineStageStarted)      => Effect.none
          case (_: Failed, _: EngineStageFailed)       => Effect.none
          case (_: Failed, _: EngineStageCompleted)    => Effect.none
          case (_: Failed, _: EngineRunSucceeded)      => Effect.none
          case (_: Failed, _: EngineRunFailed)         => Effect.none
          case (_: Completed, _: StageProgressEvent)   => Effect.none
          case (_: Failed, _: StageProgressEvent)      => Effect.none

          // ---- Already idle/completed/failed, reject new Start ----
          case (s, StartExecution(_, _, _, _, _, replyTo)) =>
            ctx.log.warn(
              s"[FabPipelineExecutionActor:$entityId] Rejecting StartExecution in state ${s.getClass.getSimpleName}"
            )
            replyTo ! Rejected(s"Already in state ${s.getClass.getSimpleName}")
            Effect.none

          case _ =>
            Effect.unhandled
        }
      }

      val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
        command match {
          case ChildCallback(owner, callback) if activeChildOwner.contains(owner) =>
            handleCommand(state, callback)
          case _: ChildCallback => Effect.none
          case _: EngineStageStarted | _: EngineStageFailed | _: EngineStageCompleted |
               _: EngineRecovering | _: EngineRunSucceeded | _: EngineRunFailed |
               _: EngineConfirmationUnknown | _: EngineStartReplied | ChildStopped |
               _: StageProgressEvent => Effect.none
          case other => handleCommand(state, other)
        }
      }

      EventSourcedBehavior[Command, Event, State](
        persistenceId = persistenceId,
        emptyState = Idle,
        commandHandler = commandHandler,
        eventHandler = eventHandler
      ).withTagger(_ => Set(Tag))
        .withRecovery(Recovery.default)
        .snapshotWhen { (_, event, _) =>
          event match {
            case StageCompleted(phase, _, _, _) =>
              phase.startsWith("Measure") || phase == "Classify" || phase == "M35ClassifyWithOcap"
            case _ => false
          }
        }
        .receiveSignal {
          case (_: RestoredExecution, AkkaRecoveryCompleted) => ctx.self ! ReconcileRun
          case (_: Executing, AkkaRecoveryCompleted) => ctx.self ! ReconcileRun

          case _ => ()
        }
    }
  }

  // ============================================================
  // Event Handler (unchanged)
  // ============================================================

  private val eventHandler: (State, Event) => State = { (state, event) =>
    (state, event) match {
      case (RestoredExecution(execution, bytes), nextEvent) =>
        eventHandler(execution, nextEvent) match {
          case next: Executing => RestoredExecution(next, bytes)
          case next: AwaitingRecovery => next.copy(planBytes = bytes)
          case other => other
        }

      case (Idle, StartedWithInput(scenarioId, workOrderId, initial, plan, bytes)) =>
        RestoredExecution(Executing(scenarioId, workOrderId, Nil, plan.size, Some(initial)), bytes)

      case (Idle, Started(scenarioId, workOrderId, stageCount)) =>
        Executing(scenarioId, workOrderId, completedPhases = Nil, stageCount = stageCount)

      case (e: Executing, StageStarted(_, _)) =>
        e

      case (e: Executing, StageCompleted(phase, _, _, _)) if e.completedPhases.contains(phase) => e
      case (e: Executing, OcapHandled(phase, _, _, _)) if e.completedPhases.contains(phase) => e

      case (e: Executing, StageCompleted(phase, _, _, Some(fabState))) =>
        e.copy(completedPhases = e.completedPhases :+ phase, fabDemoState = Some(fabState))

      case (e: Executing, StageCompleted(phase, _, _, None)) =>
        e.copy(completedPhases = e.completedPhases :+ phase)

      case (e: Executing, StageFailed(_, _, _)) =>
        e

      case (e: Executing, OcapHandled(phase, _, _, fabState)) =>
        e.copy(completedPhases = e.completedPhases :+ phase, fabDemoState = Some(fabState))

      case (e: Executing, StageProgress(_, _, _, _)) =>
        e

      case (e: Executing, AllCompleted(_, _, _, _, _, _)) =>
        Completed(e.scenarioId, e.workOrderId)

      case (e: Executing, RecoveryRequired(phase, error, _)) =>
        AwaitingRecovery(e, phase, error)

      case (_: Executing, ExecutionFailed(phase, reason)) =>
        Failed(phase, reason)

      // Idempotent replay: duplicate events after AllCompleted
      case (_: Completed, StageStarted(_, _)) =>
        state

      case (_: Completed, StageCompleted(_, _, _, _)) =>
        state

      case (_: Completed, StageFailed(_, _, _)) =>
        state

      case (_: Completed, StageProgress(_, _, _, _)) =>
        state

      case _ =>
        state
    }
  }

  // ============================================================
  // Public: convenience factory for sharding registration
  // ============================================================


  def init(
      sharding: ClusterSharding,
      contextFactory: (String, String) => FabDemoContext,
      stateFactory: String => FabDemoState,
      stageResolver: String => Seq[PipelineStage],
      publisher: FabSimulationEvent => Unit = _ => ()
  )(implicit ec: ExecutionContext): Unit = {
    sharding.init(
      Entity(EntityKey) { entityContext =>
        apply(entityContext.entityId, contextFactory, stateFactory, stageResolver, publisher)
      }
    )
  }

  /** Default stage resolver delegates to [[FabScenarioPipeline.resolveStages]]. */
  val DefaultStageResolver: String => Seq[PipelineStage] = FabScenarioPipeline.resolveStages
}
