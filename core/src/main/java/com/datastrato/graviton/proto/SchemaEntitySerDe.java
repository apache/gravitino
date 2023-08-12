/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.rel.BaseSchema;

public class SchemaEntitySerDe implements ProtoSerDe<BaseSchema, Schema> {
  @Override
  public Schema serialize(BaseSchema schemaEntity) {
    return Schema.newBuilder()
        .setId(schemaEntity.getId())
        .setCatalogId(schemaEntity.getCatalogId())
        .setName(schemaEntity.name())
        .setAuditInfo(new AuditInfoSerDe().serialize(schemaEntity.auditInfo()))
        .build();
  }

  @Override
  public BaseSchema deserialize(Schema p) {
    return new BaseSchema.SchemaBuilder()
        .withId(p.getId())
        .withCatalogId(p.getCatalogId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()))
        .build();
  }
}
