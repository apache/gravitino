package com.datastrato.unified_catalog.schema.proto;

import io.substrait.type.proto.FromProto;
import io.substrait.type.proto.TypeProtoConverter;

class ColumnSerDe implements ProtoSerDe<com.datastrato.unified_catalog.schema.Column, Column> {

  @Override
  public Column serialize(com.datastrato.unified_catalog.schema.Column column) {
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
  public com.datastrato.unified_catalog.schema.Column deserialize(Column p) {
    com.datastrato.unified_catalog.schema.Column.Builder builder =
        new com.datastrato.unified_catalog.schema.Column.Builder();
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
