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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OssieOntologyConverter;

/**
 * The persistent object for ontology identity metadata and its current version snapshot.
 *
 * <p>The identity row owns everything that is not part of the definition content: the name, the
 * status, the version pointers and the audit information. The status lives here and nowhere else,
 * so flipping it never allocates a version.
 */
@Getter
@EqualsAndHashCode(exclude = "ontologyVersionInfoPO")
@ToString
public class OntologyPO {

  /** The initial version allocated to a newly created ontology. */
  public static final Integer INITIAL_VERSION = 1;

  private static final String AI_CONTEXT_FIELD = "ai_context";
  private static final String REQUIRES_FIELD = "requires";
  private static final String ONTOLOGY_FIELD = "ontology";
  private static final String PREFIXES_FIELD = "prefixes";

  private static final ObjectMapper JSON_MAPPER = JsonUtils.anyFieldMapper();

  private Long ontologyId;
  private String ontologyName;
  private Long metalakeId;
  private String status;
  private Integer currentVersion;
  private Integer lastVersion;
  private String auditInfo;
  private Long deletedAt;
  private OntologyVersionInfoPO ontologyVersionInfoPO;

  /** Creates an empty persistent object for MyBatis. */
  public OntologyPO() {}

  /** A Lombok builder for {@link OntologyPO}. */
  public static class OntologyPOBuilder {
    // Lombok generates the builder methods.
  }

  @lombok.Builder(setterPrefix = "with")
  private OntologyPO(
      Long ontologyId,
      String ontologyName,
      Long metalakeId,
      String status,
      Integer currentVersion,
      Integer lastVersion,
      String auditInfo,
      Long deletedAt,
      OntologyVersionInfoPO ontologyVersionInfoPO) {
    Preconditions.checkArgument(ontologyId != null, "Ontology id is required");
    Preconditions.checkArgument(
        ontologyName != null && !ontologyName.isEmpty(), "Ontology name is required");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(status != null, "Ontology status is required");
    Preconditions.checkArgument(currentVersion != null, "Current version is required");
    Preconditions.checkArgument(lastVersion != null, "Last version is required");
    Preconditions.checkArgument(auditInfo != null, "Audit info is required");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.ontologyId = ontologyId;
    this.ontologyName = ontologyName;
    this.metalakeId = metalakeId;
    this.status = status;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
    this.ontologyVersionInfoPO = ontologyVersionInfoPO;
  }

  /**
   * Converts a persistent object and its current version snapshot to an ontology entity.
   *
   * @param ontologyPO The persistent object to convert.
   * @param namespace The ontology namespace, which holds the metalake name.
   * @return The converted ontology entity.
   */
  public static OntologyEntity fromOntologyPO(OntologyPO ontologyPO, Namespace namespace) {
    try {
      OntologyVersionInfoPO versionPO = ontologyPO.getOntologyVersionInfoPO();
      Map<String, String> properties =
          versionPO.properties() == null
              ? Collections.emptyMap()
              : JSON_MAPPER.readValue(
                  versionPO.properties(),
                  JSON_MAPPER
                      .getTypeFactory()
                      .constructMapType(Map.class, String.class, String.class));

      return OntologyEntity.builder()
          .withId(ontologyPO.getOntologyId())
          .withName(versionPO.ontologyName())
          .withNamespace(namespace)
          .withComment(versionPO.ontologyComment())
          .withStatus(statusOf(ontologyPO.getStatus()))
          .withDefinition(definitionFromVersionPO(versionPO))
          .withProperties(properties)
          .withAuditInfo(JSON_MAPPER.readValue(ontologyPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize ontology JSON", e);
    }
  }

  /**
   * Initializes a new ontology identity and version-one snapshot.
   *
   * @param ontologyEntity The ontology entity.
   * @param metalakeId The owning metalake ID.
   * @param builder The identity persistent-object builder.
   * @return The initialized persistent object.
   */
  public static OntologyPO initializeOntologyPO(
      OntologyEntity ontologyEntity, Long metalakeId, OntologyPOBuilder builder) {
    return buildOntologyPO(
        ontologyEntity, metalakeId, ontologyEntity.id(), builder, INITIAL_VERSION);
  }

  /**
   * Builds the next version of an existing ontology, keeping its persisted identity.
   *
   * <p>The persisted ontology ID and metalake ID win over the values on {@code ontologyEntity},
   * because an overwrite may carry a different suggested ID while the stored row must keep its own.
   *
   * @param observedOntologyPO The currently stored persistent object.
   * @param ontologyEntity The new content.
   * @param version The version to allocate.
   * @return The replacement persistent object.
   */
  public static OntologyPO rebuildOntologyPO(
      OntologyPO observedOntologyPO, OntologyEntity ontologyEntity, Integer version) {
    return buildOntologyPO(
        ontologyEntity,
        observedOntologyPO.getMetalakeId(),
        observedOntologyPO.getOntologyId(),
        OntologyPO.builder(),
        version);
  }

  /**
   * Creates a complete version snapshot for an ontology entity.
   *
   * <p>The snapshot splits the definition JSON into its sections: the concept graph, the AI
   * context, the ontology-level {@code requires} expressions and the namespace prefixes each get
   * their own column, so a reader does not have to parse the whole definition to inspect one
   * section. The status is not part of the snapshot.
   *
   * @param ontologyEntity The ontology entity.
   * @param metalakeId The owning metalake ID.
   * @param ontologyId The ontology ID to store.
   * @param version The version to allocate.
   * @return The version snapshot persistent object.
   */
  public static OntologyVersionInfoPO initializeOntologyVersionInfoPO(
      OntologyEntity ontologyEntity, Long metalakeId, Long ontologyId, Integer version) {
    try {
      ObjectNode definition = definitionNode(ontologyEntity);
      return OntologyVersionInfoPO.builder()
          .withMetalakeId(metalakeId)
          .withOntologyId(ontologyId)
          .withVersion(version)
          .withOntologyName(ontologyEntity.name())
          .withOntologyComment(ontologyEntity.comment())
          .withAiContext(sectionOrNull(definition, AI_CONTEXT_FIELD))
          .withRequires(sectionOrNull(definition, REQUIRES_FIELD))
          .withOntology(sectionOrNull(definition, ONTOLOGY_FIELD))
          .withPrefixes(sectionOrNull(definition, PREFIXES_FIELD))
          .withProperties(
              ontologyEntity.properties().isEmpty()
                  ? null
                  : JSON_MAPPER.writeValueAsString(ontologyEntity.properties()))
          .withAuditInfo(JSON_MAPPER.writeValueAsString(ontologyEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize ontology version snapshot", e);
    }
  }

  /**
   * Builds an ontology identity persistent object and the requested complete snapshot.
   *
   * @param ontologyEntity The ontology entity.
   * @param metalakeId The owning metalake ID.
   * @param ontologyId The ontology ID to store.
   * @param builder The identity persistent-object builder.
   * @param version The version to allocate.
   * @return The built persistent object.
   */
  public static OntologyPO buildOntologyPO(
      OntologyEntity ontologyEntity,
      Long metalakeId,
      Long ontologyId,
      OntologyPOBuilder builder,
      Integer version) {
    try {
      OntologyVersionInfoPO versionPO =
          initializeOntologyVersionInfoPO(ontologyEntity, metalakeId, ontologyId, version);
      return builder
          .withOntologyId(ontologyId)
          .withOntologyName(ontologyEntity.name())
          .withMetalakeId(metalakeId)
          .withStatus(ontologyEntity.status().name())
          .withAuditInfo(JSON_MAPPER.writeValueAsString(ontologyEntity.auditInfo()))
          .withCurrentVersion(version)
          .withLastVersion(version)
          .withOntologyVersionInfoPO(versionPO)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize ontology audit info", e);
    }
  }

  private static OntologyDefinition definitionFromVersionPO(OntologyVersionInfoPO versionPO) {
    ObjectNode root = JSON_MAPPER.createObjectNode();
    setSection(root, AI_CONTEXT_FIELD, versionPO.aiContext());
    setSection(root, REQUIRES_FIELD, versionPO.requires());
    setSection(root, ONTOLOGY_FIELD, versionPO.ontology());
    setSection(root, PREFIXES_FIELD, versionPO.prefixes());
    return OssieOntologyConverter.fromDefinitionJson(root.toString());
  }

  private static Ontology.Status statusOf(String status) {
    try {
      return Ontology.Status.valueOf(status);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown ontology status: " + status, e);
    }
  }

  private static ObjectNode definitionNode(OntologyEntity ontologyEntity) {
    try {
      JsonNode node =
          JSON_MAPPER.readTree(
              OssieOntologyConverter.toDefinitionJson(ontologyEntity.definition()));
      Preconditions.checkState(
          node instanceof ObjectNode, "Serialized ontology definition must be a JSON object");
      return (ObjectNode) node;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize ontology definition", e);
    }
  }

  @Nullable
  private static String sectionOrNull(ObjectNode definition, String section) {
    JsonNode value = definition.get(section);
    return value == null ? null : value.toString();
  }

  private static void setSection(ObjectNode root, String section, @Nullable String json) {
    if (json == null) {
      return;
    }
    try {
      root.set(section, JSON_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Failed to deserialize ontology definition column '" + section + "'", e);
    }
  }
}
