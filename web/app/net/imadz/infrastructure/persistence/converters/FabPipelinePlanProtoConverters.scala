package net.imadz.infrastructure.persistence.converters

import net.imadz.application.scenario.DecisionConfig
import net.imadz.domain.routing._
import net.imadz.domain.values.{Por, PorStep}
import net.imadz.fab.model.EquipmentArea
import net.imadz.infrastructure.proto.fab_pipeline_plan._
import net.imadz.infrastructure.proto.fab_run_state._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** Public, explicit value mappings for durable pipeline input.
  * These throw on missing/unknown data; FabPipelinePlanCodec exposes Either at
  * the byte boundary. Never call the private helpers in FabRunStateCodec.
  */
object FabPipelinePlanProtoConverters {
  def toProto(rule: OcapRuleDefinition): FabPlanOcapRuleProto =
    FabPlanOcapRuleProto(
      ruleId = rule.ruleId,
      name = rule.name,
      triggerCondition = Some(toProtoCondition(rule.triggerCondition)),
      actionPlan = Some(toProtoAction(rule.actionPlan)),
      priority = rule.priority,
      maxTriggersPerLot = rule.maxTriggersPerLot,
      onMissingMetric = rule.onMissingMetric match {
        case ConservativePass => "CONSERVATIVE_PASS"
        case ConservativeReject => "CONSERVATIVE_REJECT"
        case _ => invalid("unsupported missing-metric policy")
      },
      routeId = rule.routeId
    )

  def fromProto(p: FabPlanOcapRuleProto): OcapRuleDefinition = {
    requireKnown(p.unknownFields)
    OcapRuleDefinition(p.ruleId, p.name,
      fromProtoCondition(required(p.triggerCondition, "OCAP trigger condition")),
      fromProtoAction(required(p.actionPlan, "OCAP action plan")),
      p.priority, p.maxTriggersPerLot,
      p.onMissingMetric match {
        case "CONSERVATIVE_PASS" => ConservativePass
        case "CONSERVATIVE_REJECT" => ConservativeReject
        case other => invalid(s"unknown missing-metric policy '$other'")
      }, p.routeId)
  }

  def toProto(ref: SubProcessRef): FabPlanSubProcessRefProto =
    FabPlanSubProcessRefProto(ref.nodeId, ref.label, ref.subProcessType match {
      case SendAheadPilot => "SEND_AHEAD_PILOT"
      case ReworkLoop => "REWORK_LOOP"
      case HoldRelease => "HOLD_RELEASE"
      case Sampling => "SAMPLING"
      case ScrapDowngrade => "SCRAP_DOWNGRADE"
      case _ => invalid("unsupported sub-process type")
    }, ref.params)

  def fromProto(p: FabPlanSubProcessRefProto): SubProcessRef = {
    requireKnown(p.unknownFields)
    SubProcessRef(p.nodeId, p.label, p.subProcessType match {
      case "SEND_AHEAD_PILOT" => SendAheadPilot
      case "REWORK_LOOP" => ReworkLoop
      case "HOLD_RELEASE" => HoldRelease
      case "SAMPLING" => Sampling
      case "SCRAP_DOWNGRADE" => ScrapDowngrade
      case other => invalid(s"unknown sub-process type '$other'")
    }, p.params)
  }

  def toProto(por: Por): FabPlanPorProto =
    FabPlanPorProto(por.productId, por.steps.map(toProtoStep), por.version, por.mergeBeforeWarehouse)

  def fromProto(p: FabPlanPorProto): Por = {
    requireKnown(p.unknownFields)
    Por(p.productId, p.steps.map(fromProtoStep).toList, p.version, p.mergeBeforeWarehouse)
  }

  def toProto(spec: DecisionConfig): FabPlanDecisionProto =
    FabPlanDecisionProto(bits(spec.lowerSpecNm), bits(spec.upperSpecNm),
      bits(spec.borderlineWindowNm), spec.maxReworkCount, spec.reworkRecipeId)

  def fromProto(p: FabPlanDecisionProto): DecisionConfig = {
    requireKnown(p.unknownFields)
    DecisionConfig(value(p.lowerSpecNmBits), value(p.upperSpecNmBits),
      value(p.borderlineWindowNmBits), p.maxReworkCount, p.reworkRecipeId)
  }

  /** Reuse the existing run-state action protobuf, including recursive composites. */
  def toProtoAction(action: OcapActionPlan): OcapActionPlanProto = {
    import OcapActionPlanProto.Plan
    val payload = action match {
      case OcapHold(ms, reason) => Plan.Hold(OcapHoldProto(ms, reason))
      case OcapRework(recipe, max) => Plan.Rework(OcapReworkProto(recipe, max))
      case OcapScrap(reason) => Plan.Scrap(OcapScrapProto(reason))
      case OcapNotify(reason, escalation) => Plan.Notify(OcapNotifyProto(reason, escalation))
      case OcapAdjustRecipe(recipe, offset) =>
        bits(offset) // Validate without normalizing the value.
        // Existing proto3 double omits both signs of zero. Reject the lossy
        // value rather than silently changing -0.0 to +0.0 on replay.
        if (java.lang.Double.doubleToRawLongBits(offset) == Long.MinValue)
          invalid("OcapAdjustRecipe offset -0.0 cannot round-trip through the existing action protobuf")
        Plan.AdjustRecipe(OcapAdjustRecipeProto(recipe, offset))
      case OcapComposite(actions) => Plan.Composite(OcapCompositeProto(actions.map(toProtoAction)))
      case _ => invalid("unsupported OCAP action plan")
    }
    OcapActionPlanProto().withPlan(payload)
  }

  def fromProtoAction(p: OcapActionPlanProto): OcapActionPlan = {
    import OcapActionPlanProto.Plan
    requireKnown(p.unknownFields)
    p.plan match {
      case Plan.Hold(h) =>
        requireKnown(h.unknownFields)
        OcapHold(h.durationMs, h.reason)
      case Plan.Rework(r) =>
        requireKnown(r.unknownFields)
        OcapRework(r.recipeId, r.maxCount)
      case Plan.Scrap(s) =>
        requireKnown(s.unknownFields)
        OcapScrap(s.reason)
      case Plan.Notify(n) =>
        requireKnown(n.unknownFields)
        OcapNotify(n.reason, n.escalationPath)
      case Plan.AdjustRecipe(a) =>
        requireKnown(a.unknownFields)
        val action = OcapAdjustRecipe(a.recipeId, a.offsetNm)
        toProtoAction(action) // Same finite/lossless contract on both boundaries.
        action
      case Plan.Composite(c) =>
        requireKnown(c.unknownFields)
        OcapComposite(c.actions.map(fromProtoAction).toList)
      case Plan.Empty => invalid("missing or unsupported OCAP action plan")
    }
  }

  private def toProtoCondition(condition: ConditionExpression): FabPlanConditionProto = {
    import FabPlanConditionProto.Condition
    val payload = condition match {
      case MeasurementCondition(metric, op, lower, upper, scope) =>
        Condition.Measurement(FabPlanMeasurementConditionProto(metric, comparisonCode(op),
          bits(lower), bits(upper), Some(toProtoScope(scope))))
      case AggregateCondition(conditions, logic) =>
        Condition.Aggregate(FabPlanAggregateConditionProto(conditions.map(toProtoCondition), logic match {
          case And => "AND"
          case Or => "OR"
          case Not => "NOT"
          case _ => invalid("unsupported condition logic")
        }))
      case EquipmentCondition(equipment, metric, op, lower, upper) =>
        Condition.Equipment(FabPlanEquipmentConditionProto(equipment, metric,
          comparisonCode(op), bits(lower), bits(upper)))
      case _ => invalid("unsupported condition expression")
    }
    FabPlanConditionProto().withCondition(payload)
  }

  private def fromProtoCondition(p: FabPlanConditionProto): ConditionExpression = {
    import FabPlanConditionProto.Condition
    requireKnown(p.unknownFields)
    p.condition match {
      case Condition.Measurement(m) =>
        requireKnown(m.unknownFields)
        MeasurementCondition(m.metric, comparison(m.operator), value(m.lowerBoundBits),
          value(m.upperBoundBits), fromProtoScope(required(m.waferScope, "measurement wafer scope")))
      case Condition.Aggregate(a) =>
        requireKnown(a.unknownFields)
        AggregateCondition(a.conditions.map(fromProtoCondition).toList, a.logic match {
          case "AND" => And
          case "OR" => Or
          case "NOT" => Not
          case other => invalid(s"unknown condition logic '$other'")
        })
      case Condition.Equipment(e) =>
        requireKnown(e.unknownFields)
        EquipmentCondition(e.equipmentId, e.metric, comparison(e.operator),
          value(e.lowerBoundBits), value(e.upperBoundBits))
      case Condition.Empty => invalid("missing or unsupported condition expression")
    }
  }

  private def toProtoScope(scope: WaferScope): FabPlanWaferScopeProto = {
    import FabPlanWaferScopeProto.Scope
    val payload = scope match {
      case AllWafers => Scope.AllWafers(FabPlanMarkerProto())
      case AnyWafer => Scope.AnyWafer(FabPlanMarkerProto())
      case SlotRange(from, to) => Scope.SlotRange(FabPlanSlotRangeProto(from, to))
      case _ => invalid("unsupported wafer scope")
    }
    FabPlanWaferScopeProto().withScope(payload)
  }

  private def fromProtoScope(p: FabPlanWaferScopeProto): WaferScope = {
    import FabPlanWaferScopeProto.Scope
    requireKnown(p.unknownFields)
    p.scope match {
      case Scope.AllWafers(m) => requireKnown(m.unknownFields); AllWafers
      case Scope.AnyWafer(m) => requireKnown(m.unknownFields); AnyWafer
      case Scope.SlotRange(r) => requireKnown(r.unknownFields); SlotRange(r.from, r.to)
      case Scope.Empty => invalid("missing or unsupported wafer scope")
    }
  }

  private def comparisonCode(op: ComparisonOp): String = op match {
    case GreaterThan => "GT"
    case GreaterThanOrEqual => "GE"
    case LessThan => "LT"
    case LessThanOrEqual => "LE"
    case WithinRange => "WITHIN_RANGE"
    case OutsideRange => "OUTSIDE_RANGE"
    case _ => invalid("unsupported comparison operator")
  }

  private def comparison(code: String): ComparisonOp = code match {
    case "GT" => GreaterThan
    case "GE" => GreaterThanOrEqual
    case "LT" => LessThan
    case "LE" => LessThanOrEqual
    case "WITHIN_RANGE" => WithinRange
    case "OUTSIDE_RANGE" => OutsideRange
    case other => invalid(s"unknown comparison operator '$other'")
  }

  private def toProtoStep(step: PorStep): FabPlanPorStepProto =
    FabPlanPorStepProto(step.stepId, areaId(step.equipmentArea), step.recipeId,
      step.expectedDuration.length, unitCode(step.expectedDuration.unit), step.maxRetries,
      step.fallbackAreas.map(areaId), step.reentryIndex)

  private def fromProtoStep(p: FabPlanPorStepProto): PorStep = {
    requireKnown(p.unknownFields)
    PorStep(p.stepId, area(p.equipmentAreaId), p.recipeId,
      FiniteDuration(p.durationLength, unit(p.durationUnit)), p.maxRetries,
      p.fallbackAreaIds.map(area).toList, p.reentryIndex)
  }

  private def areaId(a: EquipmentArea): String = {
    if (a == null || !EquipmentArea.all.contains(a)) invalid("unsupported equipment area")
    a.areaId
  }

  private def area(id: String): EquipmentArea =
    EquipmentArea.byId(id).getOrElse(invalid(s"unknown equipment area '$id'"))

  private def unitCode(u: TimeUnit): String = u match {
    case TimeUnit.NANOSECONDS => "NANOSECONDS"
    case TimeUnit.MICROSECONDS => "MICROSECONDS"
    case TimeUnit.MILLISECONDS => "MILLISECONDS"
    case TimeUnit.SECONDS => "SECONDS"
    case TimeUnit.MINUTES => "MINUTES"
    case TimeUnit.HOURS => "HOURS"
    case TimeUnit.DAYS => "DAYS"
    case _ => invalid("unsupported duration unit")
  }

  private def unit(code: String): TimeUnit = code match {
    case "NANOSECONDS" => TimeUnit.NANOSECONDS
    case "MICROSECONDS" => TimeUnit.MICROSECONDS
    case "MILLISECONDS" => TimeUnit.MILLISECONDS
    case "SECONDS" => TimeUnit.SECONDS
    case "MINUTES" => TimeUnit.MINUTES
    case "HOURS" => TimeUnit.HOURS
    case "DAYS" => TimeUnit.DAYS
    case other => invalid(s"unknown duration unit '$other'")
  }

  private def bits(d: Double): Long = {
    if (!java.lang.Double.isFinite(d)) invalid("non-finite numeric parameter is not durable")
    java.lang.Double.doubleToRawLongBits(d)
  }

  private def value(b: Long): Double = {
    val d = java.lang.Double.longBitsToDouble(b)
    bits(d)
    d
  }

  private def required[A](field: Option[A], name: String): A =
    field.getOrElse(invalid(s"missing $name"))

  private def requireKnown(fields: scalapb.UnknownFieldSet): Unit =
    if (fields.serializedSize != 0) invalid("unsupported protobuf fields in pipeline plan")

  private def invalid(reason: String): Nothing = throw new IllegalArgumentException(reason)
}
