package net.imadz.infrastructure.persistence

import akka.serialization.SerializerWithStringManifest
import net.imadz.application.chain.FabPipelineExecutionActor.{Event, State}
import net.imadz.infrastructure.persistence.converters.FabPipelineProtoConverters
import net.imadz.infrastructure.proto.fab_pipeline.{FabPipelineEventProto, FabPipelineStateProto}

/** Narrow host serializer: no generic commands, ActorRefs or runtime contexts. */
final class FabPipelineSerializer extends SerializerWithStringManifest {
  override val identifier: Int = 77102122

  override def manifest(value: AnyRef): String = value match {
    case _: Event => "fab-pipeline-event-v1"
    case _: State => "fab-pipeline-state-v1"
    case _ => throw new IllegalArgumentException(s"Unsupported Fab pipeline value: ${value.getClass.getName}")
  }

  override def toBinary(value: AnyRef): Array[Byte] = value match {
    case event: Event => FabPipelineProtoConverters.toProto(event).toByteArray
    case state: State => FabPipelineProtoConverters.toProto(state).toByteArray
    case _ => throw new IllegalArgumentException(s"Unsupported Fab pipeline value: ${value.getClass.getName}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case "fab-pipeline-event-v1" => FabPipelineProtoConverters.fromProto(FabPipelineEventProto.parseFrom(bytes))
    case "fab-pipeline-state-v1" => FabPipelineProtoConverters.fromProto(FabPipelineStateProto.parseFrom(bytes))
    case _ => throw new IllegalArgumentException(s"Unknown Fab pipeline manifest: $manifest")
  }
}
