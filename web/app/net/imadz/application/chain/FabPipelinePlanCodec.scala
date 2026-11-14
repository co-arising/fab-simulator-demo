package net.imadz.application.chain

import net.imadz.application.chain.FabScenarioPipeline._
import net.imadz.infrastructure.persistence.converters.FabPipelinePlanProtoConverters
import net.imadz.infrastructure.proto.fab_pipeline_plan._

import scala.util.control.NonFatal

/** Versioned, data-only serialization of the actual finite input stage queue.
  *
  * Store these bytes with StartedWithInput BEFORE spawning/starting the run.
  * On recovery decode the persisted bytes, never resolve a scenario to a new
  * plan. Runtime context and checkpoints belong to their existing codecs.
  * Stable cursors can be derived by the caller from decoded stage order using
  * FabPipelineProcessor.stageName(stage) + "#" + index.
  *
  * Branch embeds Function1 and is deliberately rejected, even for familiar
  * predicates. ConditionalBranch persists only explicitly mapped NamedCondition
  * values and recursively encoded arms; no closure hashing, toString or reflection.
  */
object FabPipelinePlanCodec {
  private val SchemaVersion = 1
  private val values = FabPipelinePlanProtoConverters

  def encode(stages: Seq[PipelineStage]): Either[String, Array[Byte]] = attempt("encode") {
    if (stages == null) invalid("missing pipeline stages")
    val input = stages.toVector
    val proto = FabPipelinePlanProto(SchemaVersion,
      input.zipWithIndex.map { case (stage, index) => atStage(index)(toProto(stage)) })
    val bytes = proto.toByteArray
    // Exercise the same parser as recovery. In particular this fails closed for
    // descriptors exceeding protobuf's recursion limit, instead of persisting
    // bytes that cannot later be read by this codec.
    val restored = decode(bytes).fold(invalid, identity)
    if (restored != input) invalid("pipeline plan is not losslessly representable")
    bytes
  }

  def decode(bytes: Array[Byte]): Either[String, Vector[PipelineStage]] = attempt("decode") {
    if (bytes == null) invalid("missing pipeline plan bytes")
    val proto = FabPipelinePlanProto.parseFrom(bytes)
    known(proto.unknownFields)
    if (proto.schemaVersion != SchemaVersion)
      invalid(s"unsupported pipeline plan schema version ${proto.schemaVersion}")
    proto.stages.zipWithIndex.map { case (stage, index) => atStage(index)(fromProto(stage)) }.toVector
  }

  private def toProto(stage: PipelineStage): FabPipelinePlanStageProto = {
    import FabPipelinePlanStageProto.Stage
    val payload = stage match {
      case LoadFoup => Stage.LoadFoup(FabPlanMarkerProto())
      case Transport(from, to) => Stage.Transport(FabPlanTransportProto(from, to))
      case AtEquipment(area, equipment) => Stage.AtEquipment(FabPlanAtEquipmentProto(area, equipment))
      case TrackIn(equipment, port) => Stage.TrackIn(FabPlanEquipmentPortProto(equipment, port))
      case RunRecipe(equipment, recipe) => Stage.RunRecipe(FabPlanRecipeProto(equipment, recipe))
      case TrackOut(equipment, port) => Stage.TrackOut(FabPlanEquipmentPortProto(equipment, port))
      case Measure(equipment) => Stage.Measure(FabPlanEquipmentProto(equipment))
      case Classify => Stage.Classify(FabPlanMarkerProto())
      case SagaSplit(lot) => Stage.SagaSplit(FabPlanLotKeyProto(lot))
      case SagaMerge(lot) => Stage.SagaMerge(FabPlanLotKeyProto(lot))
      case ScrapWafers => Stage.ScrapWafers(FabPlanMarkerProto())
      case HoldWafers => Stage.HoldWafers(FabPlanMarkerProto())
      case ReleaseWafers => Stage.ReleaseWafers(FabPlanMarkerProto())
      case PostReleaseClassify => Stage.PostReleaseClassify(FabPlanMarkerProto())
      case WaitForReview(ms) => Stage.WaitForReview(FabPlanWaitProto(ms))
      case SealComplete => Stage.SealComplete(FabPlanMarkerProto())
      case Branch(_, _, _) => invalid(
        "Branch contains Function1 and is not durable; use ConditionalBranch with an explicit data-only NamedCondition")
      case ConditionalBranch(condition, ifTrue, ifFalse) =>
        Stage.ConditionalBranch(FabPlanConditionalBranchProto(
          condition = toProtoCondition(condition),
          ifTrue = toProtoArm("ifTrue", ifTrue),
          ifFalse = toProtoArm("ifFalse", ifFalse)))
      case PilotSubFlow => Stage.PilotSubFlow(FabPlanMarkerProto())
      case OcapEvaluate(rules) => Stage.OcapEvaluate(FabPlanOcapRulesProto(rules.map(values.toProto)))
      case ExecuteSubProcess(ref) => Stage.ExecuteSubProcess(values.toProto(ref))
      case M35ClassifyWithOcap(rules) => Stage.M35ClassifyWithOcap(FabPlanOcapRulesProto(rules.map(values.toProto)))
      case OcapActionRouter => Stage.OcapActionRouter(FabPlanMarkerProto())
      case AwaitSubLotResult(lot) => Stage.AwaitSubLotResult(FabPlanLotKeyProto(lot))
      case PhotoCellReworkPipeline => Stage.PhotoCellReworkPipeline(FabPlanMarkerProto())
      case DynamicPorExecution(routing, spec) =>
        Stage.DynamicPorExecution(FabPlanDynamicPorProto(Some(values.toProto(routing)), Some(values.toProto(spec))))
      case null => invalid("missing pipeline stage")
      case _ => invalid("unsupported pipeline stage; an explicit durable descriptor is required")
    }
    FabPipelinePlanStageProto().withStage(payload)
  }

  private def fromProto(p: FabPipelinePlanStageProto): PipelineStage = {
    import FabPipelinePlanStageProto.Stage
    known(p.unknownFields)
    p.stage match {
      case Stage.LoadFoup(m) => known(m.unknownFields); LoadFoup
      case Stage.Transport(t) => known(t.unknownFields); Transport(t.from, t.to)
      case Stage.AtEquipment(e) => known(e.unknownFields); AtEquipment(e.area, e.equipmentId)
      case Stage.TrackIn(e) => known(e.unknownFields); TrackIn(e.equipmentId, e.portId)
      case Stage.RunRecipe(r) => known(r.unknownFields); RunRecipe(r.equipmentId, r.recipeId)
      case Stage.TrackOut(e) => known(e.unknownFields); TrackOut(e.equipmentId, e.portId)
      case Stage.Measure(e) => known(e.unknownFields); Measure(e.equipmentId)
      case Stage.Classify(m) => known(m.unknownFields); Classify
      case Stage.SagaSplit(l) => known(l.unknownFields); SagaSplit(l.lotKey)
      case Stage.SagaMerge(l) => known(l.unknownFields); SagaMerge(l.lotKey)
      case Stage.ScrapWafers(m) => known(m.unknownFields); ScrapWafers
      case Stage.HoldWafers(m) => known(m.unknownFields); HoldWafers
      case Stage.ReleaseWafers(m) => known(m.unknownFields); ReleaseWafers
      case Stage.PostReleaseClassify(m) => known(m.unknownFields); PostReleaseClassify
      case Stage.WaitForReview(w) => known(w.unknownFields); WaitForReview(w.durationMs)
      case Stage.SealComplete(m) => known(m.unknownFields); SealComplete
      case Stage.ConditionalBranch(branch) =>
        known(branch.unknownFields)
        val condition = branch.condition match {
          case FabPlanNamedConditionProto.PILOT_PASSED => PilotPassed
          case FabPlanNamedConditionProto.REVIEW_APPROVED => ReviewApproved
          case _ => invalid("missing or unsupported branch condition")
        }
        ConditionalBranch(condition, branch.ifTrue.map(fromProto).toVector, branch.ifFalse.map(fromProto).toVector)
      case Stage.PilotSubFlow(m) => known(m.unknownFields); PilotSubFlow
      case Stage.OcapEvaluate(r) => known(r.unknownFields); OcapEvaluate(r.rules.map(values.fromProto).toList)
      case Stage.ExecuteSubProcess(r) => ExecuteSubProcess(values.fromProto(r))
      case Stage.M35ClassifyWithOcap(r) => known(r.unknownFields); M35ClassifyWithOcap(r.rules.map(values.fromProto).toList)
      case Stage.OcapActionRouter(m) => known(m.unknownFields); OcapActionRouter
      case Stage.AwaitSubLotResult(l) => known(l.unknownFields); AwaitSubLotResult(l.lotKey)
      case Stage.PhotoCellReworkPipeline(m) => known(m.unknownFields); PhotoCellReworkPipeline
      case Stage.DynamicPorExecution(d) =>
        known(d.unknownFields)
        DynamicPorExecution(values.fromProto(d.routing.getOrElse(invalid("missing dynamic POR routing"))),
          values.fromProto(d.spec.getOrElse(invalid("missing dynamic POR decision config"))))
      case Stage.Empty => invalid("missing or unsupported pipeline stage descriptor")
    }
  }

  private def toProtoCondition(condition: NamedCondition): FabPlanNamedConditionProto = condition match {
    case PilotPassed => FabPlanNamedConditionProto.PILOT_PASSED
    case ReviewApproved => FabPlanNamedConditionProto.REVIEW_APPROVED
    case _ => invalid("missing or unsupported branch condition")
  }

  private def toProtoArm(name: String, stages: Seq[PipelineStage]): Vector[FabPipelinePlanStageProto] = {
    if (stages == null) invalid(s"missing branch $name arm")
    stages.zipWithIndex.map { case (stage, index) => atStage(index)(toProto(stage)) }.toVector
  }

  private def known(fields: scalapb.UnknownFieldSet): Unit =
    if (fields.serializedSize != 0) invalid("unsupported protobuf fields in pipeline plan")

  private def atStage[A](index: Int)(body: => A): A =
    try body catch {
      case NonFatal(ex) => invalid(s"stage[$index]: ${reason(ex)}")
    }

  private def attempt[A](operation: String)(body: => A): Either[String, A] =
    try Right(body) catch {
      case NonFatal(ex) => Left(s"Fab pipeline plan $operation failed: ${reason(ex)}")
    }

  private def reason(ex: Throwable): String =
    Option(ex.getMessage).filter(_.nonEmpty).getOrElse("invalid pipeline plan data")

  private def invalid(message: String): Nothing = throw new IllegalArgumentException(message)
}
