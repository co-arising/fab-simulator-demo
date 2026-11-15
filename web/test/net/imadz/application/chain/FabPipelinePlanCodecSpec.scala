package net.imadz.application.chain

import net.imadz.application.chain.FabExecutionModel.FabDemoState
import net.imadz.application.chain.FabScenarioPipeline._
import net.imadz.application.scenario.DecisionConfig
import net.imadz.domain.routing._
import net.imadz.domain.values.{Por, PorStep}
import net.imadz.fab.model.EquipmentArea
import net.imadz.infrastructure.persistence.converters.FabPipelinePlanProtoConverters
import net.imadz.infrastructure.proto.fab_pipeline_plan._
import net.imadz.infrastructure.proto.fab_run_state.OcapActionPlanProto
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** Pure unit tests: no actor system, database, bootstrap or shared server. */
class FabPipelinePlanCodecSpec extends AnyWordSpec with Matchers {
  private val decision = DecisionConfig(27.125, 36.875, 1.625, 7, "CUSTOM-REWORK")
  private val rule = OcapRuleDefinition(
    "rule-id", "rule name",
    AggregateCondition(List(
      MeasurementCondition("custom_metric", OutsideRange, -0.0, 51.25, SlotRange(2, 7)),
      AggregateCondition(List(EquipmentCondition("EQP-X", "error_count", GreaterThanOrEqual, 4.5, 9.75)), Not)
    ), And),
    OcapComposite(List(
      OcapHold(987654321L, "review reason"), OcapRework("RECIPE-CUSTOM", 6),
      OcapScrap("scrap reason"), OcapNotify("notify reason", "engineer/escalation"),
      OcapAdjustRecipe("ADJUST-CUSTOM", -2.125), OcapComposite(Nil)
    )),
    priority = -4, maxTriggersPerLot = 11, onMissingMetric = ConservativeReject, routeId = "route-v9"
  )

  private def encode(stages: Seq[PipelineStage]): Array[Byte] =
    FabPipelinePlanCodec.encode(stages).fold(message => fail(message), identity)

  private def roundTrip(stages: Seq[PipelineStage]): Vector[PipelineStage] =
    FabPipelinePlanCodec.decode(encode(stages)).fold(message => fail(message), identity)

  private def decodeStage(stage: FabPipelinePlanStageProto): Either[String, Vector[PipelineStage]] =
    FabPipelinePlanCodec.decode(FabPipelinePlanProto(1, Vector(stage)).toByteArray)

  "FabPipelinePlanCodec" should {
    "round-trip every data-only stage case, preserving queue order, duplicates and non-default parameters" in {
      val routing = Por("PRODUCT-X", List(
        PorStep("step-X", EquipmentArea.Lithography, "RECIPE-X", 1501.nanos,
          maxRetries = 8, fallbackAreas = List(EquipmentArea.Etch, EquipmentArea.Metrology), reentryIndex = 4),
        PorStep("step-Y", EquipmentArea.Deposition, "RECIPE-Y", 13.minutes,
          maxRetries = 0, fallbackAreas = List(EquipmentArea.CMP), reentryIndex = 7)
      ), version = 42, mergeBeforeWarehouse = false)
      val stages: Vector[PipelineStage] = Vector(
        LoadFoup, Transport("FROM-X", "TO-Y"), AtEquipment("CUSTOM-AREA", "EQP-A"),
        TrackIn("EQP-B", "PORT-7"), RunRecipe("EQP-C", "RECIPE-Z"), TrackOut("EQP-D", "PORT-9"),
        Measure("EQP-E"), Classify, SagaSplit("child-alpha"), SagaMerge("child-beta"),
        ScrapWafers, HoldWafers, ReleaseWafers, PostReleaseClassify, WaitForReview(987654321L),
        SealComplete, PilotSubFlow, OcapEvaluate(List(rule)),
        ExecuteSubProcess(SubProcessRef("node-custom", "custom label", ReworkLoop,
          Map("recipeId" -> "CUSTOM-RWK", "maxCount" -> "9", "empty" -> "", "extra" -> "value"))),
        M35ClassifyWithOcap(List(rule.copy(ruleId = "second-rule"))), OcapActionRouter,
        AwaitSubLotResult("custom-await-child"), PhotoCellReworkPipeline,
        DynamicPorExecution(routing, decision), Classify, WaitForReview(0L)
      )
      roundTrip(stages) shouldBe stages
    }

    "round-trip all OCAP operators, scopes, logic, missing policies and rule order" in {
      val operators = List(GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, WithinRange, OutsideRange)
      val scopes = List(AllWafers, AnyWafer, SlotRange(-3, 15))
      val rules = operators.zipWithIndex.flatMap { case (op, index) =>
        scopes.map(scope => rule.copy(ruleId = s"rule-$index", name = "", routeId = "",
          triggerCondition = MeasurementCondition("", op, -12.25, Double.MaxValue, scope),
          onMissingMetric = ConservativePass))
      } ++ List(
        rule.copy(triggerCondition = AggregateCondition(Nil, Or), actionPlan = OcapComposite(Nil)),
        rule.copy(triggerCondition = MeasurementCondition("defaults", GreaterThan))
      )
      roundTrip(Vector(OcapEvaluate(rules), M35ClassifyWithOcap(rules.reverse))) shouldBe
        Vector(OcapEvaluate(rules), M35ClassifyWithOcap(rules.reverse))
    }

    "round-trip all sub-process types with the entire parameter map" in {
      val types = List(SendAheadPilot, ReworkLoop, HoldRelease, Sampling, ScrapDowngrade)
      val stages = types.map(t => ExecuteSubProcess(SubProcessRef("", "label", t,
        Map("" -> "", "unicode" -> "晶圆", "rule" -> "not-a-default"))))
      roundTrip(stages) shouldBe stages.toVector
    }

    "preserve POR version, area and fallback order, duration length/unit, and every decision field" in {
      val units = Vector(TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS,
        TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS)
      val steps = EquipmentArea.all.zipWithIndex.map { case (area, i) =>
        PorStep(s"step-$i", area, s"recipe-$i", FiniteDuration(i + 1L, units(i % units.size)),
          maxRetries = i, fallbackAreas = EquipmentArea.all.reverse.toList, reentryIndex = i + 3)
      }.toList
      val routing = Por("non-default-product", steps, version = 17, mergeBeforeWarehouse = false)
      val actual = roundTrip(Vector(DynamicPorExecution(routing, decision)))
      actual shouldBe Vector(DynamicPorExecution(routing, decision))
      actual.head match {
        case DynamicPorExecution(restored, spec) =>
          restored.steps.map(s => (s.expectedDuration.length, s.expectedDuration.unit)) shouldBe
            steps.map(s => (s.expectedDuration.length, s.expectedDuration.unit))
          spec shouldBe decision
        case _ => fail("Expected the dynamic POR stage")
      }
    }

    "preserve signed-zero threshold parameters rather than normalize them through proto3 doubles" in {
      val original = decision.copy(lowerSpecNm = -0.0, upperSpecNm = 0.0, borderlineWindowNm = -0.0)
      val stage = DynamicPorExecution(Por("empty", Nil, 0, false), original)
      roundTrip(Vector(stage)).head match {
        case DynamicPorExecution(_, restored) =>
          java.lang.Double.doubleToRawLongBits(restored.lowerSpecNm) shouldBe Long.MinValue
          java.lang.Double.doubleToRawLongBits(restored.upperSpecNm) shouldBe 0L
          java.lang.Double.doubleToRawLongBits(restored.borderlineWindowNm) shouldBe Long.MinValue
        case _ => fail("Expected the dynamic POR stage")
      }
    }

    "preserve an empty queue and empty rule lists and not inject default parameters" in {
      roundTrip(Vector.empty) shouldBe Vector.empty
      val stages = Vector(OcapEvaluate(Nil), M35ClassifyWithOcap(Nil), TrackIn("", ""),
        TrackOut("", ""), WaitForReview(Long.MinValue))
      roundTrip(stages) shouldBe stages
    }

    "reject every Function-backed Branch without invoking its predicate, even with empty arms" in {
      var invoked = false
      val branch = Branch((_: FabDemoState) => { invoked = true; throw new AssertionError("must not evaluate") },
        Seq.empty, Seq.empty)
      val result = FabPipelinePlanCodec.encode(Vector(LoadFoup, branch, SealComplete))
      result.isLeft shouldBe true
      result.left.toOption.get should include ("stage[1]")
      result.left.toOption.get should include ("Branch contains Function1")
      result.left.toOption.get should include ("NamedCondition")
      invoked shouldBe false
      roundTrip(FabScenarioPipeline.sendAheadStages) shouldBe FabScenarioPipeline.sendAheadStages.toVector
      roundTrip(FabScenarioPipeline.holdReleaseStages) shouldBe FabScenarioPipeline.holdReleaseStages.toVector
    }

    "round-trip named conditions and recursive arms without evaluating business predicates" in {
      val stages = Vector(ConditionalBranch(PilotPassed,
        Vector(Measure("CUSTOM"), ConditionalBranch(ReviewApproved, Vector(SealComplete), Vector(HoldWafers))),
        Vector(ScrapWafers)))
      roundTrip(stages) shouldBe stages
      PilotPassed.evaluate(FabDemoState(Map.empty, pilotPassed = true)) shouldBe true
      ReviewApproved.evaluate(FabDemoState(Map.empty)) shouldBe false
      decodeStage(FabPipelinePlanStageProto().withConditionalBranch(FabPlanConditionalBranchProto())).isLeft shouldBe true
    }

    "reject malformed bytes, unversioned/unknown schemas and missing stage descriptors" in {
      FabPipelinePlanCodec.decode(Array.emptyByteArray).isLeft shouldBe true
      FabPipelinePlanCodec.decode(Array[Byte](10, 5, 1)).isLeft shouldBe true
      FabPipelinePlanCodec.decode(FabPipelinePlanProto(0).toByteArray).isLeft shouldBe true
      FabPipelinePlanCodec.decode(FabPipelinePlanProto(99).toByteArray).isLeft shouldBe true
      decodeStage(FabPipelinePlanStageProto()).isLeft shouldBe true
      FabPipelinePlanCodec.decode(null).isLeft shouldBe true
      FabPipelinePlanCodec.encode(null).isLeft shouldBe true
      FabPipelinePlanCodec.encode(Vector(null)).isLeft shouldBe true
    }

    "reject unknown stage tags and extra unknown fields rather than replay a known subset" in {
      // stage field 27: length-delimited unknown future variant, empty payload.
      val unknown = FabPipelinePlanStageProto.parseFrom(Array[Byte](0xda.toByte, 0x01, 0x00))
      decodeStage(unknown).isLeft shouldBe true
      val knownWithUnknown = FabPipelinePlanStageProto.parseFrom(
        FabPipelinePlanStageProto().withLoadFoup(FabPlanMarkerProto()).toByteArray ++
          Array[Byte](0xda.toByte, 0x01, 0x00))
      decodeStage(knownWithUnknown).isLeft shouldBe true
      // field 100 on the root, varint value 1.
      FabPipelinePlanCodec.decode(encode(Vector(LoadFoup)) ++ Array[Byte](0xa0.toByte, 0x06, 0x01)).isLeft shouldBe true
    }

    "reject missing or unknown OCAP data and sub-process codes without fallback defaults" in {
      val original = FabPipelinePlanProto.parseFrom(encode(Vector(OcapEvaluate(List(rule)))))
        .stages.head.getOcapEvaluate.rules.head
      def ocap(p: FabPlanOcapRuleProto): Either[String, Vector[PipelineStage]] =
        decodeStage(FabPipelinePlanStageProto().withOcapEvaluate(FabPlanOcapRulesProto(Vector(p))))
      ocap(original.copy(triggerCondition = None)).isLeft shouldBe true
      ocap(original.copy(actionPlan = None)).isLeft shouldBe true
      ocap(original.copy(actionPlan = Some(OcapActionPlanProto()))).isLeft shouldBe true
      ocap(original.copy(onMissingMetric = "UNKNOWN")).isLeft shouldBe true
      ocap(original.copy(triggerCondition = Some(FabPlanConditionProto()))).isLeft shouldBe true
      ocap(original.copy(triggerCondition = Some(FabPlanConditionProto().withMeasurement(
        FabPlanMeasurementConditionProto("m", "UNKNOWN", 0L, 0L,
          Some(FabPlanWaferScopeProto().withAnyWafer(FabPlanMarkerProto()))))))).isLeft shouldBe true
      ocap(original.copy(triggerCondition = Some(FabPlanConditionProto().withMeasurement(
        FabPlanMeasurementConditionProto("m", "GT", 0L, 0L, None))))).isLeft shouldBe true
      ocap(original.copy(triggerCondition = Some(FabPlanConditionProto().withAggregate(
        FabPlanAggregateConditionProto(Vector.empty, "UNKNOWN"))))).isLeft shouldBe true
      decodeStage(FabPipelinePlanStageProto().withExecuteSubProcess(
        FabPlanSubProcessRefProto("node", "label", "UNKNOWN"))).isLeft shouldBe true
    }

    "reject missing dynamic POR data, unknown equipment areas or duration units" in {
      def dynamic(p: FabPlanDynamicPorProto): Either[String, Vector[PipelineStage]] =
        decodeStage(FabPipelinePlanStageProto().withDynamicPorExecution(p))
      val base = FabPipelinePlanProto.parseFrom(encode(Vector(
        DynamicPorExecution(Por("P", List(PorStep("S", EquipmentArea.Etch, "R", 1.second))), decision))))
        .stages.head.getDynamicPorExecution
      dynamic(base.copy(routing = None)).isLeft shouldBe true
      dynamic(base.copy(spec = None)).isLeft shouldBe true
      val routing = base.getRouting
      val step = routing.steps.head
      dynamic(base.copy(routing = Some(routing.copy(steps = Vector(step.copy(equipmentAreaId = "UNKNOWN")))))).isLeft shouldBe true
      dynamic(base.copy(routing = Some(routing.copy(steps = Vector(step.copy(durationUnit = "UNKNOWN")))))).isLeft shouldBe true
      dynamic(base.copy(routing = Some(routing.copy(steps = Vector(step.copy(fallbackAreaIds = Vector("UNKNOWN"))))))).isLeft shouldBe true
    }

    "reject non-finite values and the existing action protobuf's lossy negative-zero offset" in {
      val routing = Por("P", Nil)
      List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { n =>
        FabPipelinePlanCodec.encode(Vector(DynamicPorExecution(routing, decision.copy(lowerSpecNm = n)))).isLeft shouldBe true
        FabPipelinePlanCodec.encode(Vector(OcapEvaluate(List(rule.copy(
          triggerCondition = EquipmentCondition("E", "M", GreaterThan, n)))))).isLeft shouldBe true
        FabPipelinePlanCodec.encode(Vector(OcapEvaluate(List(rule.copy(
          actionPlan = OcapAdjustRecipe("R", n)))))).isLeft shouldBe true
      }
      FabPipelinePlanCodec.encode(Vector(OcapEvaluate(List(rule.copy(
        actionPlan = OcapAdjustRecipe("R", -0.0)))))).isLeft shouldBe true
      val invalidSpec = FabPipelinePlanProto.parseFrom(encode(Vector(DynamicPorExecution(routing, decision))))
        .stages.head.getDynamicPorExecution
      decodeStage(FabPipelinePlanStageProto().withDynamicPorExecution(invalidSpec.copy(spec = Some(
        invalidSpec.getSpec.copy(lowerSpecNmBits = java.lang.Double.doubleToRawLongBits(Double.NaN)))))).isLeft shouldBe true
    }

    "expose reusable public OCAP, POR, decision and sub-process value converters" in {
      val converters = FabPipelinePlanProtoConverters
      converters.fromProto(converters.toProto(rule)) shouldBe rule
      converters.fromProtoAction(converters.toProtoAction(rule.actionPlan)) shouldBe rule.actionPlan
      val ref = SubProcessRef("node", "label", Sampling, Map("key" -> "value"))
      converters.fromProto(converters.toProto(ref)) shouldBe ref
      val por = Por("P", List(PorStep("S", EquipmentArea.Logistics, "R", 17.micros)), 8, false)
      converters.fromProto(converters.toProto(por)) shouldBe por
      converters.fromProto(converters.toProto(decision)) shouldBe decision
    }
  }
}
