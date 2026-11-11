/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Schema;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.MaskAndOmitKeys;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;

/**
 * A Schema class to represent a schema metadata object that combines the metadata both from {@link
 * Schema} and {@link SchemaEntity}.
 */
public final class EntityCombinedSchema implements Schema {

  private final Schema schema;

  private final SchemaEntity schemaEntity;

  // Editable hidden/secret keys are masked; reserved+hidden keys are omitted.
  private Set<String> keysToMask = Collections.emptySet();

  private Set<String> keysToOmit = Collections.emptySet();

  // Field "imported" is used to indicate whether the entity has been imported to Gravitino
  // managed storage backend. If "imported" is true, it means that storage backend have stored
  // the correct entity. Otherwise, we should import the external entity to the storage backend.
  // This is used for tag/access control related purposes, only the imported entities have the
  // unique id, and based on this id, we can label and control the access to the entities.
  private boolean imported;

  private EntityCombinedSchema(Schema schema, SchemaEntity schemaEntity) {
    this.schema = schema;
    this.schemaEntity = schemaEntity;
    this.imported = false;
  }

  public SchemaEntity schemaEntity() {
    return schemaEntity;
  }

  public Schema schema() {
    return schema;
  }

  public static EntityCombinedSchema of(Schema schema, SchemaEntity schemaEntity) {
    return new EntityCombinedSchema(schema, schemaEntity);
  }

  public static EntityCombinedSchema of(Schema schema) {
    return of(schema, null);
  }

  public EntityCombinedSchema withHiddenProperties(MaskAndOmitKeys keys) {
    MaskAndOmitKeys classification = keys == null ? MaskAndOmitKeys.empty() : keys;
    this.keysToMask = classification.keysToMask();
    this.keysToOmit = classification.keysToOmit();
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
    return HiddenPropertyMaskUtils.maskHiddenProperties(
        schema.properties(), keysToMask, keysToOmit);
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
