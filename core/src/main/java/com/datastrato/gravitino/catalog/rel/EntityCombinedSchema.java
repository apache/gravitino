/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.rel;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import java.util.Map;

/**
 * A Schema class to represent a schema metadata object that combines the metadata both from {@link
 * Schema} and {@link SchemaEntity}.
 */
public final class EntityCombinedSchema implements Schema {

  private final Schema schema;
  private final SchemaEntity schemaEntity;

  private EntityCombinedSchema(Schema schema, SchemaEntity schemaEntity) {
    this.schema = schema;
    this.schemaEntity = schemaEntity;
  }

  public static EntityCombinedSchema of(Schema schema, SchemaEntity schemaEntity) {
    return new EntityCombinedSchema(schema, schemaEntity);
  }

  public static EntityCombinedSchema of(Schema schema) {
    return of(schema, null);
  }

  @Override
  public String name() {
    return schema.name();
  }

  @Override
  public String comment() {
    return schema.comment();
  }

  @Override
  public Map<String, String> properties() {
    return schema.properties();
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        new AuditInfo.Builder()
            .withCreator(schema.auditInfo().creator())
            .withCreateTime(schema.auditInfo().createTime())
            .withLastModifier(schema.auditInfo().lastModifier())
            .withLastModifiedTime(schema.auditInfo().lastModifiedTime())
            .build();

    return schemaEntity == null
        ? schema.auditInfo()
        : mergedAudit.merge(schemaEntity.auditInfo(), true /* overwrite */);
  }
}
