/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.rel;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.SchemaEntity;
import com.datastrato.graviton.rel.Schema;
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
    return ((AuditInfo) schema.auditInfo()).merge(schemaEntity.auditInfo(), true /* overwrite */);
  }
}
