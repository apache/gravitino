/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.SchemaEntity;

public class SchemaEntitySerDe implements ProtoSerDe<SchemaEntity, Schema> {
  @Override
  public Schema serialize(SchemaEntity schemaEntity) {
    return Schema.newBuilder()
        .setId(schemaEntity.id())
        .setName(schemaEntity.name())
        .setAuditInfo(new AuditInfoSerDe().serialize(schemaEntity.auditInfo()))
        .build();
  }

  @Override
  public SchemaEntity deserialize(Schema p) {
    return new SchemaEntity.Builder()
        .withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()))
        .build();
  }
}
