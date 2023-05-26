package com.datastrato.graviton.schema.proto;

import io.substrait.type.proto.FromProto;
import io.substrait.type.proto.TypeProtoConverter;

class ColumnSerDe implements ProtoSerDe<com.datastrato.graviton.schema.Column, Column> {

  @Override
  public Column serialize(com.datastrato.graviton.schema.Column column) {
    Column.Builder builder = Column.newBuilder();
    builder
        .setId(column.getId())
        .setName(column.getName())
        .setEntityId(column.getEntityId())
        .setEntitySnapshotId(column.getEntitySnapshotId())
        .setType(column.getType().accept(TypeProtoConverter.INSTANCE))
        .setPosition(column.getPosition())
        .setAuditInfo(new AuditInfoSerDe().serialize(column.auditInfo()));

    if (column.getComment() != null) {
      builder.setComment(column.getComment());
    }

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.schema.Column deserialize(Column p) {
    com.datastrato.graviton.schema.Column.Builder builder =
        new com.datastrato.graviton.schema.Column.Builder();
    builder
        .withId(p.getId())
        .withName(p.getName())
        .withEntityId(p.getEntityId())
        .withEntitySnapshotId(p.getEntitySnapshotId())
        .withType(FromProto.from(p.getType()))
        .withPosition(p.getPosition())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    return builder.build();
  }
}
