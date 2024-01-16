/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.SchemaEntity;

public class SchemaEntitySerDe implements ProtoSerDe<SchemaEntity, Schema> {
  @Override
  public Schema serialize(SchemaEntity schemaEntity) {
    Schema.Builder builder =
        Schema.newBuilder()
            .setId(schemaEntity.id())
            .setName(schemaEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(schemaEntity.auditInfo()));

    if (schemaEntity.comment() != null) {
      builder.setComment(schemaEntity.comment());
    }

    if (schemaEntity.properties() != null && !schemaEntity.properties().isEmpty()) {
      builder.putAllProperties(schemaEntity.properties());
    }

    return builder.build();
  }

  @Override
  public SchemaEntity deserialize(Schema p) {
    SchemaEntity.Builder builder =
        new SchemaEntity.Builder()
            .withId(p.getId())
            .withName(p.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
