package com.datastrato.unified_catalog.schema.proto;

import com.datastrato.unified_catalog.schema.Entity;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ProtoUtils {
  private static final Map<
          Class<? extends Entity>, ProtoSerDe<? extends Entity, ? extends MessageLite>>
      ENTITY_TO_SERDE;

  static {
    Map<Class<? extends Entity>, ProtoSerDe<? extends Entity, ? extends MessageLite>> tmp =
        new HashMap<>();
    tmp.put(com.datastrato.unified_catalog.schema.AuditInfo.class, new AuditInfoSerDe());
    tmp.put(com.datastrato.unified_catalog.schema.Tenant.class, new TenantSerDe());
    tmp.put(com.datastrato.unified_catalog.schema.Lakehouse.class, new LakehouseSerDe());
    tmp.put(com.datastrato.unified_catalog.schema.Zone.class, new ZoneSerDe());
    tmp.put(com.datastrato.unified_catalog.schema.Table.class, new TableSerDe());
    tmp.put(com.datastrato.unified_catalog.schema.Column.class, new ColumnSerDe());

    ENTITY_TO_SERDE = Collections.unmodifiableMap(tmp);
  }

  private static final Map<
          Class<? extends MessageLite>, ProtoSerDe<? extends Entity, ? extends MessageLite>>
      PROTO_TO_SERDE;

  static {
    Map<Class<? extends MessageLite>, ProtoSerDe<? extends Entity, ? extends MessageLite>> tmp =
        new HashMap<>();
    tmp.put(AuditInfo.class, new AuditInfoSerDe());
    tmp.put(Tenant.class, new TenantSerDe());
    tmp.put(Lakehouse.class, new LakehouseSerDe());
    tmp.put(Zone.class, new ZoneSerDe());
    tmp.put(Table.class, new TableSerDe());
    tmp.put(Column.class, new ColumnSerDe());

    PROTO_TO_SERDE = Collections.unmodifiableMap(tmp);
  }

  private ProtoUtils() {}

  public static Timestamp fromInstant(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  public static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  public static <T extends Entity, M extends MessageLite> M toProto(T t) {
    if (!ENTITY_TO_SERDE.containsKey(t.getClass())) {
      throw new ProtoSerDeException("No proto serializer for " + t.getClass().getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) ENTITY_TO_SERDE.get(t.getClass());
    return protoSerDe.serialize(t);
  }

  public static <T extends Entity> byte[] toBytes(T t) {
    return toProto(t).toByteArray();
  }

  public static <T extends Entity, M extends MessageLite> T fromProto(M m) {
    if (!PROTO_TO_SERDE.containsKey(m.getClass())) {
      throw new ProtoSerDeException("No proto deserializer for " + m.getClass().getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) PROTO_TO_SERDE.get(m.getClass());
    return protoSerDe.deserialize(m);
  }

  public static <T extends Entity, M extends MessageLite> T fromBytes(
      byte[] bytes, Class<T> clazz, Parser<M> parser) {
    if (!ENTITY_TO_SERDE.containsKey(clazz)) {
      throw new ProtoSerDeException("No proto deserializer for " + clazz.getName());
    }

    try {
      M message = parser.parseFrom(bytes);
      ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) ENTITY_TO_SERDE.get(clazz);
      return protoSerDe.deserialize(message);

    } catch (Exception e) {
      throw new ProtoSerDeException("Failed in deserializing bytes to proto", e);
    }
  }
}
