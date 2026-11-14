package net.imadz.infrastructure.persistence

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.remote.serialization.ProtobufSerializer
import akka.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import net.imadz.domain.entities.LotEntity._
import net.imadz.infrastructure.proto.lot._
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.UUID

object LotJournalSerializerSpec {
  // Use application.conf AND dependency reference.conf bindings. Only isolate
  // actor infrastructure; no test-only serializers, Play bootstrap, DB or ports.
  val config = ConfigFactory.parseString("""
    akka.actor.provider = local
    akka.actor.allow-java-serialization = off
    akka.remote.artery.canonical.port = 0
    akka.cluster.seed-nodes = []
  """).withFallback(ConfigFactory.load())
}

/** Wire/adapter regression, not an actor, journal-plugin or JVM-recovery test. */
class LotJournalSerializerSpec
    extends ScalaTestWithActorTestKit(LotJournalSerializerSpec.config)
    with AnyWordSpecLike {

  private val serialization = SerializationExtension(system.classicSystem)
  private val events = new LotEventAdapter
  private val snapshots = new LotSnapshotAdapter
  private val lotId = UUID.fromString("00000000-0000-0000-0000-000000000001")
  private val wafer1 = UUID.fromString("00000000-0000-0000-0000-000000000011")
  private val wafer2 = UUID.fromString("00000000-0000-0000-0000-000000000012")
  private val wafer3 = UUID.fromString("00000000-0000-0000-0000-000000000013")
  private val wafer4 = UUID.fromString("00000000-0000-0000-0000-000000000014")
  private val transfer1 = UUID.fromString("00000000-0000-0000-0000-000000000021")
  private val transfer2 = UUID.fromString("00000000-0000-0000-0000-000000000022")
  private val childId = UUID.fromString("00000000-0000-0000-0000-000000000031")
  private val carried = Map(
    wafer1 -> WaferState("晶圆-A", WaferActive, Some("PASS"), 2, Some(31.125), measured = true),
    wafer2 -> WaferState("HOLD-B", WaferOnHold, Some("FAIL"), 3, Some(37.875), measured = true),
    wafer3 -> WaferState("SCRAP-C", WaferScrapped, Some("SCRAP"), 4, Some(42.5), measured = true),
    wafer4 -> WaferState("SKIP-D", WaferSkipped)
  )
  private val route = RouteCard(Vector("Measure", "Rework", "Measure", "Classify"), 2,
    Some("route-X:v9"), "OCAP-001:Rework", 9876543210123L)
  private val base = LotState(lotId, "PRODUCT-X")
  private val wireManifest = "lot-event-v1"
  private def lotSerializer: LotJournalSerializer =
    serialization.findSerializerFor(LotEventPO.Event.LotSealed(LotSealedPO())) match {
      case serializer: LotJournalSerializer => serializer
      case other => fail(s"Runtime selected ${other.getClass.getName}, not LotJournalSerializer")
    }

  private def roundTripJournal(journal: LotEventPO.Event): LotEventPO.Event = {
    val serializer = serialization.findSerializerFor(journal)
    serializer shouldBe a[LotJournalSerializer]
    serializer.identifier shouldBe 77102123
    Serializers.manifestFor(serializer, journal) shouldBe wireManifest
    val bytes = serialization.serialize(journal).get
    // Array identity is irrelevant: compare protobuf bytes by content.
    bytes.toVector shouldBe LotEventPO(journal).toByteArray.toVector
    val restored = serialization.deserialize(bytes, serializer.identifier, wireManifest).get
      .asInstanceOf[LotEventPO.Event]
    restored shouldBe journal
    serialization.serialize(restored).get.toVector shouldBe bytes.toVector
    restored
  }

  private def roundTripEvent(event: LotEvent, expectedTag: Option[Int] = None): Unit = {
    val journal = events.toJournal(event)
    journal.isEmpty shouldBe false
    expectedTag.foreach(tag => journal.number shouldBe tag)
    val adapterManifest = events.manifest(event)
    adapterManifest shouldBe event.getClass.getName
    events.fromJournal(roundTripJournal(journal), adapterManifest).events shouldBe Seq(event)
  }

  private def roundTripSnapshot(state: LotState): LotState = {
    val journal = snapshots.toJournal(state).asInstanceOf[LotStatePO]
    val serializer = serialization.findSerializerFor(journal)
    // This is the public common-core GeneratedMessage binding, not a test alias.
    serializer shouldBe a[ProtobufSerializer]
    val bytes = serialization.serialize(journal).get
    bytes.toVector shouldBe journal.toByteArray.toVector
    val restored = serialization.deserialize(bytes, serializer.identifier,
      Serializers.manifestFor(serializer, journal)).get.asInstanceOf[LotStatePO]
    restored shouldBe journal
    serialization.serialize(restored).get.toVector shouldBe bytes.toVector
    snapshots.fromJournal(restored)
  }

  // All domain variants; old tags 1–28 stay fixed and RouteCard appends 29/30.
  // Missing support must fail, never cancel or be skipped.
  private val eventCases: Vector[(Int, LotEvent)] = Vector(
    1 -> LotCreated("PRODUCT-X", Map(wafer1 -> "晶圆-A", wafer2 -> "WAFER-B"),
      Some(childId), Some(HoldSplit), Some("WO-X")),
    2 -> WaferRemovalReserved(transfer1, Set(wafer1, wafer2), Set("晶圆-A", "WAFER-B")),
    3 -> WaferRemovalCommitted(transfer1, Set("晶圆-A", "WAFER-B")),
    4 -> WaferRemovalReleased(transfer1),
    5 -> WaferAdditionReserved(transfer1, carried.keySet, carried),
    6 -> WaferAdditionCommitted(transfer1),
    7 -> WaferAdditionCanceled(transfer1),
    8 -> PhaseStarted("PHOTO-X"),
    9 -> PhaseCompleted("PHOTO-X"),
    10 -> LotSealed(),
    11 -> FoupLoaded("FOUP-X", "STOCKER-X"),
    12 -> TransportStarted("FOUP-X", "FROM-X", "TO-Y", 9876543210123L),
    13 -> TransportCompleted("FOUP-X", "EQP-X"),
    14 -> EquipmentJobStarted("EQP-X", "RECIPE-X"),
    15 -> EquipmentJobCompleted("EQP-X", "JOB-X", success = true),
    16 -> WaferMeasured(wafer1, 31.125),
    17 -> WaferClassified(wafer1, "FAIL", 3, 37.875),
    18 -> WafersSplitForRework(Set("WAFER-A", "WAFER-B"), Set("WAFER-C"), 3),
    19 -> WafersReworked(Set("WAFER-A", "WAFER-B")),
    20 -> WafersSentAsPilot(Set("WAFER-A", "WAFER-B")),
    21 -> WafersSampled(Set("WAFER-A"), Set("WAFER-B", "WAFER-C")),
    22 -> WafersHeld(Set("WAFER-A", "WAFER-B"), "工程复核"),
    23 -> WafersReleased(Set("WAFER-A", "WAFER-B")),
    24 -> ProcessCompleted("LOT-X", 7, 2, 3),
    25 -> LotFailed("equipment failed", "Measure#2"),
    26 -> SubLotCreated(childId, SampleSplit, Set(wafer1, wafer2)),
    27 -> SubLotMerged(childId, Set(wafer1, wafer2)),
    28 -> SubLotScrapped(childId, "scrap reason", Set(wafer1, wafer2)),
    29 -> RouteCardAssigned(route.steps, route.sourcedFrom, route.reason, route.assignedAt),
    30 -> RouteCardStepAdvanced(7)
  )

  "Lot journal runtime serialization" should {
    "load the public application and common bindings with Java serialization disabled" in {
      val config = system.settings.config
      config.getBoolean("akka.actor.allow-java-serialization") shouldBe false
      config.getString("akka.actor.provider") shouldBe "local"
      config.getString("akka.actor.serialization-bindings.\"net.imadz.infrastructure.proto.lot.LotEventPO$Event\"") shouldBe
        "lot-journal-proto"
      config.getString("akka.actor.serialization-bindings.\"scalapb.GeneratedMessage\"") shouldBe "proto"
      info(s"Runtime bindings: event=${lotSerializer.getClass.getName}, " +
        s"snapshot=${serialization.findSerializerFor(LotStatePO(lotId = lotId.toString)).getClass.getName}")
      Vector(classOf[LotJournalSerializer], classOf[LotEventAdapter], classOf[LotSnapshotAdapter],
        classOf[net.imadz.infrastructure.persistence.converters.LotProtoConverters], classOf[LotState])
        .foreach(clazz => info(s"Loaded ${clazz.getName} from ${clazz.getProtectionDomain.getCodeSource.getLocation}"))
    }

    "cover every current protobuf oneof tag without renumbering the old wire" in {
      val descriptorTags = LotEventPO.scalaDescriptor.fields.map(_.number).toSet
      eventCases.map(_._1).toSet shouldBe descriptorTags
      eventCases.map(_._2.getClass).distinct.size shouldBe eventCases.size
    }

    "keep all historical wafer and snapshot field numbers and types" in {
      val legacyWafer = Map(
        "wafer_id" -> 1, "name" -> 2, "status" -> 3, "classification" -> 4,
        "rework_count" -> 5, "cd_value" -> 6, "measured" -> 7
      )
      val legacySnapshot = Map(
        "lot_id" -> 1, "product_id" -> 2, "wafer_ids" -> 3, "reserved_wafers" -> 4,
        "incoming_wafers" -> 5, "phase" -> 6, "completed_transfer_ids" -> 7,
        "current_step_index" -> 8, "area_visit_history" -> 9, "routing_step_reentry" -> 10,
        "loaded_foup_id" -> 11, "completed_jobs" -> 12, "measured_wafers" -> 13,
        "wafer_classifications" -> 14, "parent_lot_id" -> 15, "split_reason" -> 16,
        "work_order_id" -> 17
      )
      WaferStatePO.scalaDescriptor.fields.filter(_.number <= 7)
        .map(field => field.name -> field.number).toMap shouldBe legacyWafer
      LotStatePO.scalaDescriptor.fields.filter(_.number <= 17)
        .map(field => field.name -> field.number).toMap shouldBe legacySnapshot
      // Fixed old bytes protect both tags and scalar wire types from changing.
      WaferStatePO(waferId = "id", name = "n", status = "Active", classification = "PASS",
        reworkCount = 2, cdValue = 1.0, measured = true).toByteArray.toVector shouldBe
        Vector[Byte](0x0a, 0x02, 0x69, 0x64, 0x12, 0x01, 0x6e,
          0x1a, 0x06, 0x41, 0x63, 0x74, 0x69, 0x76, 0x65,
          0x22, 0x04, 0x50, 0x41, 0x53, 0x53, 0x28, 0x02,
          0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0.toByte, 0x3f, 0x38, 0x01)
    }

    eventCases.foreach { case (tag, event) =>
      s"round-trip ${event.getClass.getSimpleName} through adapter, runtime wire and adapter (tag $tag)" in {
        roundTripEvent(event, Some(tag))
      }
    }

    "round-trip RouteCardAssigned including ordered duplicate steps and provenance" in {
      roundTripEvent(RouteCardAssigned(route.steps, route.sourcedFrom, route.reason, route.assignedAt), Some(29))
      Vector(None, Some("")).foreach { source =>
        roundTripEvent(RouteCardAssigned(Vector.empty, source, "", 0L), Some(29))
      }
    }

    "round-trip RouteCardStepAdvanced with a non-default index" in {
      roundTripEvent(RouteCardStepAdvanced(7), Some(30))
      roundTripEvent(RouteCardStepAdvanced(0), Some(30))
    }

    "preserve default optional event fields, false flags and every split reason" in {
      roundTripEvent(LotCreated("PRODUCT-X", Map.empty))
      roundTripEvent(WaferAdditionReserved(transfer1, Set.empty))
      roundTripEvent(EquipmentJobCompleted("EQP-X", "JOB-X", success = false))
      Vector(ReworkSplit, ScrapSplit, PilotSplit, SampleSplit, HoldSplit).foreach { reason =>
        roundTripEvent(LotCreated("PRODUCT-X", Map(wafer1 -> "WAFER-A"), Some(childId), Some(reason)))
        roundTripEvent(SubLotCreated(childId, reason, Set.empty))
      }
    }

    "reject an unknown serializer manifest even for valid bytes" in {
      val bytes = serialization.serialize(events.toJournal(LotSealed())).get
      val error = serialization.deserialize(bytes, lotSerializer.identifier, "unknown-manifest").failed.get
      error shouldBe a[IllegalArgumentException]
      error.getMessage should include ("Unknown lot journal manifest")
    }

    "reject empty oneofs on encoding and missing or future-only payloads on decoding" in {
      serialization.serialize(LotEventPO.Event.Empty).failed.get shouldBe a[IllegalArgumentException]
      Vector(Array.emptyByteArray, LotEventPO().toByteArray,
        Array[Byte](0xa2.toByte, 0x06, 0x00)).foreach { bytes => // unknown field 100
        serialization.deserialize(bytes, lotSerializer.identifier, wireManifest).failed.get shouldBe
          a[IllegalArgumentException]
      }
      // Empty payload is invalid, but a PRESENT LotSealed marker is valid.
      roundTripEvent(LotSealed())
      events.fromJournal(LotEventPO.Event.Empty, "").events shouldBe empty
    }

    "reject unsupported serializer inputs rather than fall back to Java serialization" in {
      val serializer = lotSerializer
      Vector[AnyRef]("not-a-lot-oneof", LotEventPO(), LotSealed(), null).foreach { unsupported =>
        intercept[IllegalArgumentException](serializer.manifest(unsupported))
        intercept[IllegalArgumentException](serializer.toBinary(unsupported))
      }
    }

    "reject malformed protobuf bytes" in {
      serialization.deserialize(Array[Byte](0x0a, 0x05, 0x01), lotSerializer.identifier, wireManifest)
        .failed.get shouldBe a[com.google.protobuf.InvalidProtocolBufferException]
    }
  }

  "Old Lot protobuf fields" should {
    "decode LotCreated with only deprecated wafer_ids and leave new optionals absent" in {
      val old = LotEventPO.Event.LotCreated(LotCreatedPO("LEGACY", Vector(wafer1.toString, wafer2.toString)))
      events.fromJournal(roundTripJournal(old), "legacy-class-name").events shouldBe
        Seq(LotCreated("LEGACY", Map(wafer1 -> "", wafer2 -> "")))
    }

    "prefer wafer_names over deprecated wafer_ids when both are present" in {
      val old = LotEventPO.Event.LotCreated(LotCreatedPO("LEGACY", Vector(wafer2.toString),
        Vector(WaferNameEntryPO(wafer1.toString, "kept-name"))))
      events.fromJournal(roundTripJournal(old), "").events shouldBe
        Seq(LotCreated("LEGACY", Map(wafer1 -> "kept-name")))
    }

    "decode WaferAdditionReserved without the later carried_wafers field" in {
      val old = LotEventPO.Event.WaferAdditionReserved(
        WaferAdditionReservedPO(transfer1.toString, Vector(wafer1.toString)))
      events.fromJournal(roundTripJournal(old), "").events shouldBe
        Seq(WaferAdditionReserved(transfer1, Set(wafer1)))
    }

    "decode both historical and current carried-wafer status spellings" in {
      Vector("Active" -> WaferActive, "OnHold" -> WaferOnHold, "Scrapped" -> WaferScrapped,
        "Skipped" -> WaferSkipped, "WaferActive" -> WaferActive, "WaferOnHold" -> WaferOnHold,
        "WaferScrapped" -> WaferScrapped, "WaferSkipped" -> WaferSkipped).foreach { case (name, status) =>
        val old = LotEventPO.Event.WaferAdditionReserved(WaferAdditionReservedPO(transfer1.toString,
          Vector(wafer1.toString), Vector(WaferStatePO(waferId = wafer1.toString, name = "old", status = name))))
        events.fromJournal(roundTripJournal(old), "").events shouldBe
          Seq(WaferAdditionReserved(transfer1, Set(wafer1), Map(wafer1 -> WaferState("old", status))))
      }
    }

    "retain legacy scalar defaults without claiming to recover missing wafer Options" in {
      val oldCases = Vector(
        WaferStatePO(waferId = wafer1.toString, name = "old") -> WaferState("old"),
        WaferStatePO(waferId = wafer1.toString, name = "old", measured = true) ->
          WaferState("old", cdValue = Some(0.0), measured = true),
        WaferStatePO(waferId = wafer1.toString, name = "old", cdValue = 31.125) ->
          WaferState("old", cdValue = Some(31.125))
      )
      oldCases.foreach { case (wire, expected) =>
        val old = LotEventPO.Event.WaferAdditionReserved(WaferAdditionReservedPO(
          transferId = transfer1.toString, waferIds = Vector(wafer1.toString), carriedWafers = Vector(wire)))
        events.fromJournal(roundTripJournal(old), "").events shouldBe
          Seq(WaferAdditionReserved(transfer1, Set(wafer1), Map(wafer1 -> expected)))
      }
    }

    "prefer present wafer Options over contradictory deprecated scalars" in {
      val wire = WaferStatePO(waferId = wafer1.toString, name = "new", classification = "FAIL",
        reworkCount = 2, cdValue = 37.875, measured = true, options = Some(WaferStateOptionsPO()))
      val journal = LotEventPO.Event.WaferAdditionReserved(WaferAdditionReservedPO(
        transferId = transfer1.toString, waferIds = Vector(wafer1.toString), carriedWafers = Vector(wire)))
      events.fromJournal(roundTripJournal(journal), "").events shouldBe
        Seq(WaferAdditionReserved(transfer1, Set(wafer1),
          Map(wafer1 -> WaferState("new", reworkCount = 2, measured = true))))
    }

    "reconstruct a legacy snapshot from ids, classifications and measured flags" in {
      val old = LotStatePO(lotId = lotId.toString, productId = "LEGACY", waferIds = Vector(wafer1.toString),
        phase = "Active", measuredWafers = Vector(wafer1.toString),
        waferClassifications = Vector(WaferClassResultPO(wafer1.toString, "FAIL", 37.875, 3)),
        currentStepIndex = 4, areaVisitHistory = Vector("MET", "LITHO"),
        routingStepReentry = Vector(IntEntry("LITHO", 2)), completedJobs = Vector("old-job"))
      val serializer = serialization.findSerializerFor(old)
      serializer shouldBe a[ProtobufSerializer]
      val restored = serialization.deserialize(serialization.serialize(old).get, serializer.identifier,
        Serializers.manifestFor(serializer, old)).get.asInstanceOf[LotStatePO]
      restored shouldBe old // old wire-only fields must survive protobuf transport
      snapshots.fromJournal(restored) shouldBe LotState(lotId, "LEGACY",
        wafers = Map(wafer1 -> WaferState("WAFER-1", classification = Some("FAIL"), reworkCount = 3,
          cdValue = Some(37.875), measured = true)), phase = Active)
    }
    "prefer full snapshot state over legacy derived fields, including an explicitly empty map" in {
      val wireWafer = WaferStatePO(waferId = wafer2.toString, name = "kept-name",
        status = "WaferSkipped", reworkCount = 4, options = Some(WaferStateOptionsPO(
          cdValue = Some(LotDoubleValuePO(value = 0.0)))))
      val mixed = LotStatePO(lotId = lotId.toString, productId = "MIXED",
        waferIds = Vector(wafer1.toString), measuredWafers = Vector(wafer1.toString),
        waferClassifications = Vector(WaferClassResultPO(wafer1.toString, "FAIL", 37.875, 3)),
        loadedFoupId = "legacy-foup", workOrderId = "legacy-work-order", currentStepIndex = 7,
        waferStates = Some(WaferStateMapPO(wafers = Map(wafer2.toString -> wireWafer))),
        options = Some(LotStateOptionsPO()))
      val serializer = serialization.findSerializerFor(mixed)
      def decode(wire: LotStatePO): LotState = {
        val restored = serialization.deserialize(serialization.serialize(wire).get, serializer.identifier,
          Serializers.manifestFor(serializer, wire)).get.asInstanceOf[LotStatePO]
        snapshots.fromJournal(restored)
      }
      decode(mixed) shouldBe LotState(lotId, "MIXED", wafers = Map(wafer2 ->
        WaferState("kept-name", WaferSkipped, reworkCount = 4, cdValue = Some(0.0))))
      decode(mixed.copy(waferStates = Some(WaferStateMapPO()))) shouldBe LotState(lotId, "MIXED")
    }

    "leave newly stored snapshot facts absent when decoding a default legacy snapshot" in {
      val old = LotStatePO(lotId = lotId.toString, productId = "LEGACY",
        waferIds = Vector(wafer1.toString), currentStepIndex = 7)
      snapshots.fromJournal(old) shouldBe LotState(lotId, "LEGACY",
        wafers = Map(wafer1 -> WaferState("WAFER-1")))
      snapshots.fromJournal(old.copy(loadedFoupId = "legacy-foup", workOrderId = "legacy-work-order")) shouldBe
        LotState(lotId, "LEGACY", wafers = Map(wafer1 -> WaferState("WAFER-1")),
          loadedFoupId = Some("legacy-foup"), workOrderId = Some("legacy-work-order"))
    }
  }

  "Lot snapshot runtime serialization and adapter" should {
    "round-trip an empty state and every phase and split reason" in {
      roundTripSnapshot(base) shouldBe base
      Vector(Empty, Active, Sealed, Completed, AwaitingSubLot).foreach { phase =>
        roundTripSnapshot(base.copy(phase = phase)) shouldBe base.copy(phase = phase)
      }
      Vector(ReworkSplit, ScrapSplit, PilotSplit, SampleSplit, HoldSplit).foreach { reason =>
        val state = base.copy(parentLotId = Some(childId), splitReason = Some(reason))
        roundTripSnapshot(state) shouldBe state
      }
    }

    "preserve every currently representable domain field" in {
      val state = base.copy(wafers = Map(wafer1 -> WaferState("WAFER-1", classification = Some("FAIL"),
        reworkCount = 3, cdValue = Some(37.875), measured = true)),
        reservedWafers = Map(transfer1 -> Set(wafer1, wafer2), transfer2 -> Set.empty),
        incomingWafers = Map(transfer2 -> Set(wafer3, wafer4)), phase = AwaitingSubLot,
        completedTransferIds = Set(transfer1, transfer2), loadedFoupId = Some("FOUP-X"),
        parentLotId = Some(childId), splitReason = Some(HoldSplit), workOrderId = Some("WO-X"))
      roundTripSnapshot(state) shouldBe state
    }

    "preserve wafer names rather than invent positional names" in {
      val state = base.copy(wafers = Map(wafer1 -> WaferState("晶圆-A")))
      roundTripSnapshot(state) shouldBe state
    }

    "preserve every wafer status" in {
      Vector(WaferActive, WaferOnHold, WaferScrapped, WaferSkipped).foreach { status =>
        val state = base.copy(wafers = Map(wafer1 -> WaferState("WAFER-1", status)))
        withClue(s"status=$status: ") { roundTripSnapshot(state) shouldBe state }
      }
    }

    "preserve measurements and rework counts even before classification" in {
      val state = base.copy(wafers = Map(wafer1 -> WaferState("WAFER-1", reworkCount = 2,
        cdValue = Some(31.125), measured = true)))
      roundTripSnapshot(state) shouldBe state
    }

    "preserve wafer Option presence independently of scalar defaults and measured flags" in {
      for {
        classification <- Vector(None, Some(""), Some("PASS"))
        cdValue <- Vector(None, Some(0.0), Some(31.125))
        measured <- Vector(false, true)
      } {
        val wafer = WaferState("晶圆-zero", WaferOnHold, classification, 2, cdValue, measured)
        val state = base.copy(wafers = Map(wafer1 -> wafer),
          incomingCarriedWafers = Map(transfer1 -> Map(wafer1 -> wafer)))
        withClue(s"classification=$classification cdValue=$cdValue measured=$measured: ") {
          roundTripEvent(WaferAdditionReserved(transfer1, Set(wafer1), Map(wafer1 -> wafer)), Some(5))
          roundTripSnapshot(state) shouldBe state
        }
      }
    }

    "preserve reserved wafer names" in {
      val state = base.copy(reservedWaferNames = Map(transfer1 -> Set("晶圆-A", "WAFER-B"),
        transfer2 -> Set.empty))
      roundTripSnapshot(state) shouldBe state
    }

    "preserve incoming carried wafer state for each reserved transfer" in {
      val state = base.copy(incomingWafers = Map(transfer1 -> carried.keySet),
        incomingCarriedWafers = Map(transfer1 -> carried, transfer2 -> Map.empty))
      roundTripSnapshot(state) shouldBe state
    }

    "preserve the entire route card including current index, duplicates, provenance and timestamp" in {
      val state = base.copy(routeCard = Some(route))
      roundTripSnapshot(state) shouldBe state
      Vector(None, Some("")).foreach { source =>
        val zero = base.copy(routeCard = Some(RouteCard(Vector.empty, 0, source, "", 0L)))
        roundTripSnapshot(zero) shouldBe zero
      }
    }

    "preserve empty and absent optional snapshot strings distinctly" in {
      for {
        foup <- Vector(None, Some(""), Some("FOUP-X"))
        workOrder <- Vector(None, Some(""), Some("WO-X"))
      } {
        val state = base.copy(loadedFoupId = foup, workOrderId = workOrder)
        roundTripSnapshot(state) shouldBe state
      }
    }

    "round-trip a snapshot with every domain field populated" in {
      val state = base.copy(wafers = carried,
        reservedWafers = Map(transfer1 -> Set(wafer1, wafer2)),
        reservedWaferNames = Map(transfer1 -> Set("晶圆-A", "HOLD-B")),
        incomingWafers = Map(transfer2 -> Set(wafer3, wafer4)),
        incomingCarriedWafers = Map(transfer2 -> carried.filter { case (id, _) => id == wafer3 || id == wafer4 }),
        phase = AwaitingSubLot, completedTransferIds = Set(transfer1), loadedFoupId = Some("FOUP-X"),
        parentLotId = Some(childId), splitReason = Some(ReworkSplit), routeCard = Some(route),
        workOrderId = Some("WO-X"))
      roundTripSnapshot(state) shouldBe state
    }

    "reject unsupported snapshot journal types" in {
      intercept[IllegalStateException](snapshots.fromJournal(LotEventPO()))
    }
  }
}
