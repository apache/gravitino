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
package org.apache.gravitino.storage.relational.po;

import static org.apache.gravitino.storage.relational.utils.POConverters.DEFAULT_DELETED_AT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.relational.service.EntityIdService;

/** The persistent object for Semantic Model identity metadata and its current version snapshot. */
@Getter
@EqualsAndHashCode(exclude = "semanticModelVersionInfoPO")
@ToString
public class SemanticModelPO {

  /** The initial version allocated to a newly created Semantic Model. */
  public static final Long INITIAL_VERSION = 1L;

  private Long semanticModelId;
  private String semanticModelName;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;
  private SemanticModelVersionInfoPO semanticModelVersionInfoPO;

  /** Creates an empty persistent object for MyBatis. */
  public SemanticModelPO() {}

  /** A Lombok builder for {@link SemanticModelPO}. */
  public static class SemanticModelPOBuilder {
    // Lombok generates the builder methods.
  }

  @lombok.Builder(setterPrefix = "with")
  private SemanticModelPO(
      Long semanticModelId,
      String semanticModelName,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt,
      SemanticModelVersionInfoPO semanticModelVersionInfoPO) {
    Preconditions.checkArgument(semanticModelId != null, "Semantic Model id is required");
    Preconditions.checkArgument(semanticModelName != null, "Semantic Model name is required");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(auditInfo != null, "Audit info is required");
    Preconditions.checkArgument(currentVersion != null, "Current version is required");
    Preconditions.checkArgument(lastVersion != null, "Last version is required");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.semanticModelId = semanticModelId;
    this.semanticModelName = semanticModelName;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
    this.semanticModelVersionInfoPO = semanticModelVersionInfoPO;
  }

  /**
   * Converts a persistent object and its current version snapshot to a Semantic Model entity.
   *
   * @param semanticModelPO The persistent object to convert.
   * @param namespace The Semantic Model namespace.
   * @return The converted Semantic Model entity.
   */
  public static SemanticModelEntity fromSemanticModelPO(
      SemanticModelPO semanticModelPO, Namespace namespace) {
    try {
      SemanticModelVersionInfoPO versionPO = semanticModelPO.getSemanticModelVersionInfoPO();
      SemanticModelDefinition definition =
          SemanticModelDefinitionSerDe.deserialize(versionPO.semanticModelDefinition());
      Map<String, String> properties =
          versionPO.properties() == null
              ? Collections.emptyMap()
              : JsonUtils.anyFieldMapper()
                  .readValue(
                      versionPO.properties(),
                      JsonUtils.anyFieldMapper()
                          .getTypeFactory()
                          .constructMapType(Map.class, String.class, String.class));

      return SemanticModelEntity.builder()
          .withId(semanticModelPO.getSemanticModelId())
          .withName(versionPO.semanticModelName())
          .withNamespace(namespace)
          .withComment(versionPO.semanticModelComment())
          .withDefinition(definition)
          .withProperties(properties)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(semanticModelPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize Semantic Model JSON", e);
    }
  }

  /**
   * Initializes a new Semantic Model identity and version-one snapshot.
   *
   * @param semanticModelEntity The Semantic Model entity.
   * @param builder The identity persistent-object builder.
   * @return The initialized persistent object.
   */
  public static SemanticModelPO initializeSemanticModelPO(
      SemanticModelEntity semanticModelEntity, SemanticModelPOBuilder builder) {
    builder.withCurrentVersion(INITIAL_VERSION).withLastVersion(INITIAL_VERSION);
    return buildSemanticModelPO(semanticModelEntity, builder, INITIAL_VERSION.intValue());
  }

  /**
   * Creates a complete version snapshot for a Semantic Model entity.
   *
   * @param semanticModelEntity The Semantic Model entity.
   * @param namespacedEntityId The resolved schema and ancestor IDs.
   * @param version The version to allocate.
   * @return The version snapshot persistent object.
   */
  public static SemanticModelVersionInfoPO initializeSemanticModelVersionInfoPO(
      SemanticModelEntity semanticModelEntity,
      NamespacedEntityId namespacedEntityId,
      Integer version) {
    try {
      String definitionJson =
          SemanticModelDefinitionSerDe.serialize(semanticModelEntity.definition());
      String propertiesJson =
          semanticModelEntity.properties().isEmpty()
              ? null
              : JsonUtils.anyFieldMapper().writeValueAsString(semanticModelEntity.properties());

      return SemanticModelVersionInfoPO.builder()
          .withSemanticModelId(semanticModelEntity.id())
          .withMetalakeId(namespacedEntityId.namespaceIds()[0])
          .withCatalogId(namespacedEntityId.namespaceIds()[1])
          .withSchemaId(namespacedEntityId.entityId())
          .withVersion(version)
          .withSemanticModelName(semanticModelEntity.name())
          .withSemanticModelComment(semanticModelEntity.comment())
          .withSemanticModelDefinition(definitionJson)
          .withProperties(propertiesJson)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().writeValueAsString(semanticModelEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize Semantic Model JSON", e);
    }
  }

  /**
   * Builds a Semantic Model identity persistent object and the requested complete snapshot.
   *
   * @param semanticModelEntity The Semantic Model entity.
   * @param builder The identity persistent-object builder.
   * @param version The version to allocate.
   * @return The built persistent object.
   */
  public static SemanticModelPO buildSemanticModelPO(
      SemanticModelEntity semanticModelEntity, SemanticModelPOBuilder builder, Integer version) {
    try {
      NamespacedEntityId namespacedEntityId =
          EntityIdService.getEntityIds(
              NameIdentifier.of(semanticModelEntity.namespace().levels()),
              Entity.EntityType.SCHEMA);
      SemanticModelVersionInfoPO versionPO =
          initializeSemanticModelVersionInfoPO(semanticModelEntity, namespacedEntityId, version);
      return builder
          .withSemanticModelId(semanticModelEntity.id())
          .withSemanticModelName(semanticModelEntity.name())
          .withMetalakeId(namespacedEntityId.namespaceIds()[0])
          .withCatalogId(namespacedEntityId.namespaceIds()[1])
          .withSchemaId(namespacedEntityId.entityId())
          .withAuditInfo(
              JsonUtils.anyFieldMapper().writeValueAsString(semanticModelEntity.auditInfo()))
          .withSemanticModelVersionInfoPO(versionPO)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize Semantic Model audit info", e);
    }
  }
}
