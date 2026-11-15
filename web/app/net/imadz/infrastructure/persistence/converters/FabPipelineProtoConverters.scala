package net.imadz.infrastructure.persistence.converters

import net.imadz.application.chain.FabPipelineExecutionActor._
import net.imadz.application.chain.FabRunStateCodec
import net.imadz.infrastructure.proto.fab_pipeline._

/** Protobuf mapping for the parent's domain events and snapshots only. */
object FabPipelineProtoConverters {
  private val codec = FabRunStateCodec

  def toProto(event: Event): FabPipelineEventProto = {
    import FabPipelineEventProto.{Event => Payload}
    val payload = event match {
      case StartedWithInput(scenario, workOrder, initial, plan, bytes) =>
        Payload.StartedWithInput(FabPipelineStartedWithInputProto(scenario, workOrder,
          Some(codec.toProtoState(initial)), plan, com.google.protobuf.ByteString.copyFrom(bytes.toArray)))
      case Started(scenario, workOrder, count) =>
        Payload.Started(FabPipelineStartedProto(scenario, workOrder, count))
      case StageStarted(phase, timestamp) =>
        Payload.StageStarted(FabPipelineStageStartedProto(phase, timestamp))
      case StageCompleted(phase, timestamp, metadata, state) =>
        Payload.StageCompleted(FabPipelineStageCompletedProto(phase, timestamp, metadata, state.map(codec.toProtoState)))
      case StageFailed(phase, error, timestamp) =>
        Payload.StageFailed(FabPipelineStageFailedProto(phase, Some(codec.toProtoError(error)), timestamp))
      case OcapHandled(phase, error, timestamp, state) =>
        Payload.OcapHandled(FabPipelineOcapHandledProto(phase, Some(codec.toProtoError(error)), timestamp,
          Some(codec.toProtoState(state))))
      case StageProgress(status, detail, phase, timestamp) =>
        Payload.StageProgress(FabPipelineStageProgressProto(status, detail, phase, timestamp))
      case AllCompleted(scenario, workOrder, total, passed, reworked, scrapped) =>
        Payload.AllCompleted(FabPipelineAllCompletedProto(scenario, workOrder, total, passed, reworked, scrapped))
      case RecoveryRequired(phase, error, timestamp) =>
        Payload.RecoveryRequired(FabPipelineRecoveryRequiredProto(phase, Some(codec.toProtoError(error)), timestamp))
      case ExecutionFailed(phase, reason) =>
        Payload.ExecutionFailed(FabPipelineExecutionFailedProto(phase, reason))
    }
    FabPipelineEventProto().withEvent(payload)
  }

  def fromProto(proto: FabPipelineEventProto): Event = {
    import FabPipelineEventProto.{Event => Payload}
    proto.event match {
      case Payload.StartedWithInput(p) => StartedWithInput(p.scenarioId, p.workOrderId,
        codec.fromProtoState(p.initialState.getOrElse(missing("initial state"))), p.plan.toVector,
        p.planBytes.toByteArray.toVector)
      case Payload.Started(p) => Started(p.scenarioId, p.workOrderId, p.stageCount)
      case Payload.StageStarted(p) => StageStarted(p.phase, p.timestamp)
      case Payload.StageCompleted(p) => StageCompleted(p.phase, p.timestamp, p.metadata,
        p.fabState.map(codec.fromProtoState))
      case Payload.StageFailed(p) => StageFailed(p.phase,
        codec.fromProtoError(p.error.getOrElse(missing("stage error"))), p.timestamp)
      case Payload.OcapHandled(p) => OcapHandled(p.phase,
        codec.fromProtoError(p.error.getOrElse(missing("OCAP error"))), p.timestamp,
        codec.fromProtoState(p.fabState.getOrElse(missing("OCAP state"))))
      case Payload.StageProgress(p) => StageProgress(p.status, p.detail, p.phase, p.timestamp)
      case Payload.AllCompleted(p) => AllCompleted(p.scenarioId, p.workOrderId, p.totalWafers,
        p.passedWafers, p.reworkedWafers, p.scrappedWafers)
      case Payload.RecoveryRequired(p) => RecoveryRequired(p.phase,
        codec.fromProtoError(p.error.getOrElse(missing("recovery error"))), p.timestamp)
      case Payload.ExecutionFailed(p) => ExecutionFailed(p.phase, p.reason)
      case Payload.Empty => missing("event")
    }
  }

  def toProto(state: State): FabPipelineStateProto = {
    import FabPipelineStateProto.{State => Payload}
    val payload = state match {
      case Idle => Payload.Idle(FabPipelineIdleProto())
      case Executing(scenario, workOrder, phases, count, fabState) =>
        Payload.Executing(FabPipelineExecutingProto(scenario, workOrder, phases, count,
          fabState.map(codec.toProtoState)))
      case RestoredExecution(e, bytes) =>
        Payload.RestoredExecution(FabPipelineRestoredExecutionProto(
          Some(FabPipelineExecutingProto(e.scenarioId, e.workOrderId, e.completedPhases,
            e.stageCount, e.fabDemoState.map(codec.toProtoState))), com.google.protobuf.ByteString.copyFrom(bytes.toArray)))
      case AwaitingRecovery(e, phase, error, bytes) =>
        Payload.AwaitingRecovery(FabPipelineAwaitingRecoveryProto(
          Some(FabPipelineExecutingProto(e.scenarioId, e.workOrderId, e.completedPhases,
            e.stageCount, e.fabDemoState.map(codec.toProtoState))), phase, Some(codec.toProtoError(error)),
          com.google.protobuf.ByteString.copyFrom(bytes.toArray)))
      case Completed(scenario, workOrder) => Payload.Completed(FabPipelineCompletedProto(scenario, workOrder))
      case Failed(phase, reason) => Payload.Failed(FabPipelineFailedProto(phase, reason))
    }
    FabPipelineStateProto().withState(payload)
  }

  def fromProto(proto: FabPipelineStateProto): State = {
    import FabPipelineStateProto.{State => Payload}
    proto.state match {
      case Payload.Idle(_) => Idle
      case Payload.Executing(p) => Executing(p.scenarioId, p.workOrderId, p.completedPhases.toList,
        p.stageCount, p.fabState.map(codec.fromProtoState))
      case Payload.RestoredExecution(p) =>
        val e = p.execution.getOrElse(missing("restored execution"))
        RestoredExecution(Executing(e.scenarioId, e.workOrderId, e.completedPhases.toList,
          e.stageCount, e.fabState.map(codec.fromProtoState)), p.planBytes.toByteArray.toVector)
      case Payload.AwaitingRecovery(p) =>
        val e = p.execution.getOrElse(missing("recovery execution"))
        AwaitingRecovery(Executing(e.scenarioId, e.workOrderId, e.completedPhases.toList,
          e.stageCount, e.fabState.map(codec.fromProtoState)), p.phase,
          codec.fromProtoError(p.error.getOrElse(missing("recovery error"))), p.planBytes.toByteArray.toVector)
      case Payload.Completed(p) => Completed(p.scenarioId, p.workOrderId)
      case Payload.Failed(p) => Failed(p.phase, p.reason)
      case Payload.Empty => missing("state")
    }
  }

  private def missing(field: String): Nothing =
    throw new IllegalArgumentException(s"Fab pipeline protobuf is missing $field")
}
