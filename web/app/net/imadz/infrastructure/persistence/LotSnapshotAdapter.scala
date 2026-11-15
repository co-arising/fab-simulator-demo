package net.imadz.infrastructure.persistence

import akka.persistence.typed.SnapshotAdapter
import net.imadz.domain.entities.LotEntity._
import net.imadz.infrastructure.persistence.converters.LotProtoConverters
import net.imadz.infrastructure.proto.lot.LotStatePO

class LotSnapshotAdapter extends SnapshotAdapter[LotState] with LotProtoConverters {

  // Keep the GeneratedMessage journal type and its existing protobuf binding.
  override def toJournal(state: LotState): LotStatePO = LotStateConv.toProto(state)

  override def fromJournal(from: Any): LotState = from match {
    case po: LotStatePO => LotStateConv.fromProto(po)
    case unknown => throw new IllegalStateException(s"Unknown journal type: ${unknown.getClass.getName}")
  }
}
