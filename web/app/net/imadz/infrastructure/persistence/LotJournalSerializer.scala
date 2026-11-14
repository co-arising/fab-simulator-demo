package net.imadz.infrastructure.persistence

import akka.serialization.SerializerWithStringManifest
import net.imadz.infrastructure.proto.lot.LotEventPO

/** ScalaPB oneof values are not GeneratedMessage. Preserve the existing event
  * adapter's oneof journal type by encoding it in its protobuf envelope. */
final class LotJournalSerializer extends SerializerWithStringManifest {
  override val identifier: Int = 77102123

  override def manifest(value: AnyRef): String = value match {
    case _: LotEventPO.Event => "lot-event-v1"
    case _ => throw new IllegalArgumentException("Unsupported lot journal value")
  }

  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case event: LotEventPO.Event if !event.isEmpty => LotEventPO(event).toByteArray
    case _ => throw new IllegalArgumentException("Missing lot event payload")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case "lot-event-v1" =>
      val event = LotEventPO.parseFrom(bytes).event
      require(!event.isEmpty, "Missing lot event payload")
      event
    case _ => throw new IllegalArgumentException(s"Unknown lot journal manifest: $manifest")
  }
}
