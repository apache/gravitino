/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.SchemaEntity;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Schema class to represent a schema metadata object that combines the metadata both from {@link
 * Schema} and {@link SchemaEntity}.
 */
public final class EntityCombinedSchema implements Schema {

  private final Schema schema;
  private final SchemaEntity schemaEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties;
  // If imported is true, it means that storage backend have stored the correct entity.
  // Otherwise, we should import the external entity to the storage backend.
  private boolean imported;

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

  public EntityCombinedSchema withHiddenPropertiesSet(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  public EntityCombinedSchema withImported(boolean imported) {
    this.imported = imported;
    return this;
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
    return schema.properties().entrySet().stream()
        .filter(e -> !hiddenProperties.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(schema.auditInfo().creator())
            .withCreateTime(schema.auditInfo().createTime())
            .withLastModifier(schema.auditInfo().lastModifier())
            .withLastModifiedTime(schema.auditInfo().lastModifiedTime())
            .build();

    return schemaEntity == null
        ? schema.auditInfo()
        : mergedAudit.merge(schemaEntity.auditInfo(), true /* overwrite */);
  }

  public boolean imported() {
    return imported;
  }

  StringIdentifier stringIdentifier() {
    return StringIdentifier.fromProperties(schema.properties());
  }
}
