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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.relational.service.EntityIdService;

@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class FunctionPO {

  private static final Integer INITIAL_VERSION = 1;

  private Long functionId;

  private String functionName;

  private Long metalakeId;

  private Long catalogId;

  private Long schemaId;

  private String functionType;

  private Integer deterministic;

  private String returnType;

  private Integer functionLatestVersion;

  private Integer functionCurrentVersion;

  private String auditInfo;

  private Long deletedAt;

  private FunctionVersionPO functionVersionPO;

  public FunctionPO() {}

  public static class FunctionPOBuilder {
    // Lombok will generate builder methods
  }

  @lombok.Builder(setterPrefix = "with")
  private FunctionPO(
      Long functionId,
      String functionName,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      String functionType,
      Integer deterministic,
      String returnType,
      Integer functionLatestVersion,
      Integer functionCurrentVersion,
      String auditInfo,
      Long deletedAt,
      FunctionVersionPO functionVersionPO) {
    Preconditions.checkArgument(functionId != null, "Function id is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(functionName), "Function name cannot be empty");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(functionType), "Function type cannot be empty");
    Preconditions.checkArgument(
        functionLatestVersion != null, "Function latest version is required");
    Preconditions.checkArgument(
        functionCurrentVersion != null, "Function current version is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "Audit info cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");

    this.functionId = functionId;
    this.functionName = functionName;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.functionType = functionType;
    this.deterministic = deterministic;
    this.returnType = returnType;
    this.functionLatestVersion = functionLatestVersion;
    this.functionCurrentVersion = functionCurrentVersion;
    this.auditInfo = auditInfo;
    this.deletedAt = deletedAt;
    this.functionVersionPO = functionVersionPO;
  }

  // ============================ PO Converters ============================

  public static FunctionEntity fromFunctionPO(FunctionPO functionPO, Namespace namespace) {
    try {
      FunctionVersionPO versionPO = functionPO.functionVersionPO();
      List<FunctionDefinitionDTO> definitionDTOs =
          JsonUtils.anyFieldMapper()
              .readValue(
                  versionPO.definitions(),
                  JsonUtils.anyFieldMapper()
                      .getTypeFactory()
                      .constructCollectionType(List.class, FunctionDefinitionDTO.class));
      FunctionDefinition[] definitions =
          definitionDTOs.stream()
              .map(FunctionDefinitionDTO::toFunctionDefinition)
              .toArray(FunctionDefinition[]::new);
      return FunctionEntity.builder()
          .withId(functionPO.functionId())
          .withName(functionPO.functionName())
          .withNamespace(namespace)
          .withComment(versionPO.functionComment())
          .withFunctionType(FunctionType.valueOf(functionPO.functionType()))
          .withDeterministic(functionPO.deterministic() != null && functionPO.deterministic() == 1)
          .withReturnType(JsonUtils.anyFieldMapper().readValue(functionPO.returnType(), Type.class))
          .withDefinitions(definitions)
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(functionPO.auditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  public static FunctionPO initializeFunctionPO(
      FunctionEntity functionEntity, FunctionPO.FunctionPOBuilder builder) {
    builder.withFunctionLatestVersion(INITIAL_VERSION).withFunctionCurrentVersion(INITIAL_VERSION);
    return buildFunctionPO(functionEntity, builder);
  }

  public static FunctionVersionPO initializeFunctionVersionPO(
      FunctionEntity functionEntity, NamespacedEntityId namespacedEntityId, Integer version) {
    try {
      List<FunctionDefinitionDTO> definitionDTOs =
          Arrays.stream(functionEntity.definitions())
              .map(FunctionDefinitionDTO::fromFunctionDefinition)
              .collect(Collectors.toList());
      return FunctionVersionPO.builder()
          .withFunctionId(functionEntity.id())
          .withMetalakeId(namespacedEntityId.namespaceIds()[0])
          .withCatalogId(namespacedEntityId.namespaceIds()[1])
          .withSchemaId(namespacedEntityId.entityId())
          .withFunctionVersion(version)
          .withFunctionComment(functionEntity.comment())
          .withDefinitions(JsonUtils.anyFieldMapper().writeValueAsString(definitionDTOs))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  public static FunctionPO buildFunctionPO(
      FunctionEntity functionEntity, FunctionPO.FunctionPOBuilder builder) {
    try {
      NamespacedEntityId namespacedEntityId =
          EntityIdService.getEntityIds(
              NameIdentifier.of(functionEntity.namespace().levels()), Entity.EntityType.SCHEMA);
      FunctionVersionPO versionPO =
          initializeFunctionVersionPO(functionEntity, namespacedEntityId, INITIAL_VERSION);
      return builder
          .withFunctionId(functionEntity.id())
          .withFunctionName(functionEntity.name())
          .withFunctionType(functionEntity.functionType().name())
          .withDeterministic(functionEntity.deterministic() ? 1 : 0)
          .withReturnType(
              JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.returnType()))
          .withFunctionVersionPO(versionPO)
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }
}
