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

package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.meta.FunctionEntity.LATEST_VERSION;
import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;
import static org.apache.gravitino.storage.relational.utils.POConverters.DEFAULT_DELETED_AT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.FunctionMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.gravitino.storage.relational.po.FunctionVersionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionMetaService.class);

  private static final FunctionMetaService INSTANCE = new FunctionMetaService();

  public static FunctionMetaService getInstance() {
    return INSTANCE;
  }

  private FunctionMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listFunctionsByNamespace")
  public List<FunctionEntity> listFunctionsByNamespace(Namespace ns) {
    NamespaceUtil.checkFunction(ns);

    Long schemaId =
        EntityIdService.getEntityId(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);

    List<FunctionPO> functionPOs =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class, mapper -> mapper.listFunctionPOsBySchemaId(schemaId));

    return functionPOs.stream().map(f -> fromFunctionPO(f, ns)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionByVersionedIdentifier")
  public FunctionEntity getFunctionByVersionedIdentifier(NameIdentifier ident) {
    FunctionPO functionPO = getFunctionPOByVersionedIdentifier(ident);
    // Extract function namespace from versioned ident: [metalake, catalog, schema, functionName]
    Namespace functionNamespace = NameIdentifier.of(ident.namespace().levels()).namespace();
    return fromFunctionPO(functionPO, functionNamespace);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertFunction")
  public void insertFunction(FunctionEntity functionEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkFunction(functionEntity.nameIdentifier());

    try {
      FunctionPO.Builder builder = FunctionPO.builder();
      fillFunctionPOBuilderParentEntityId(builder, functionEntity.namespace());
      FunctionPO po = initializeFunctionPO(functionEntity, builder);

      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFunctionMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertFunctionMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFunctionVersionMetaOnDuplicateKeyUpdate(
                          po.getFunctionVersionPO());
                    } else {
                      mapper.insertFunctionVersionMeta(po.getFunctionVersionPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FUNCTION, functionEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunction")
  public boolean deleteFunction(NameIdentifier ident) {
    // ident is a standard function identifier: metalake.catalog.schema.functionName
    NameIdentifierUtil.checkFunction(ident);

    Long schemaId;
    try {
      schemaId =
          EntityIdService.getEntityId(
              NameIdentifier.of(ident.namespace().levels()), Entity.EntityType.SCHEMA);
      // Verify function exists
      getFunctionIdBySchemaIdAndFunctionName(schemaId, ident.name());
    } catch (NoSuchEntityException e) {
      LOG.warn("Failed to delete function: {}", ident, e);
      return false;
    }

    AtomicInteger functionDeletedCount = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        // delete function versions first
        () ->
            SessionUtils.doWithoutCommit(
                FunctionVersionMetaMapper.class,
                mapper ->
                    mapper.softDeleteFunctionVersionsBySchemaIdAndFunctionName(
                        schemaId, ident.name())),

        // delete function meta
        () ->
            functionDeletedCount.set(
                SessionUtils.getWithoutCommit(
                    FunctionMetaMapper.class,
                    mapper ->
                        mapper.softDeleteFunctionMetaBySchemaIdAndFunctionName(
                            schemaId, ident.name()))));

    return functionDeletedCount.get() > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunctionMetasByLegacyTimeline")
  public int deleteFunctionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int[] totalDeleted = {0};
    SessionUtils.doMultipleWithCommit(
        () ->
            totalDeleted[0] +=
                SessionUtils.doWithCommitAndFetchResult(
                    FunctionVersionMetaMapper.class,
                    mapper ->
                        mapper.deleteFunctionVersionMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            totalDeleted[0] +=
                SessionUtils.doWithCommitAndFetchResult(
                    FunctionMetaMapper.class,
                    mapper -> mapper.deleteFunctionMetasByLegacyTimeline(legacyTimeline, limit)));
    return totalDeleted[0];
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunctionVersionsByRetentionCount")
  public int deleteFunctionVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    List<FunctionMaxVersionPO> functionCurVersions =
        SessionUtils.getWithoutCommit(
            FunctionVersionMetaMapper.class,
            mapper -> mapper.selectFunctionVersionsByRetentionCount(versionRetentionCount));

    int totalDeletedCount = 0;
    for (FunctionMaxVersionPO functionCurVersion : functionCurVersions) {
      long versionRetentionLine = functionCurVersion.getVersion() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              FunctionVersionMetaMapper.class,
              mapper ->
                  mapper.softDeleteFunctionVersionsByRetentionLine(
                      functionCurVersion.getFunctionId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      LOG.info(
          "Soft delete functionVersions count: {} which versions are older than or equal to"
              + " versionRetentionLine: {}, the current functionId and version is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          functionCurVersion.getFunctionId(),
          functionCurVersion.getVersion());
    }
    return totalDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionIdBySchemaIdAndFunctionName")
  public Long getFunctionIdBySchemaIdAndFunctionName(Long schemaId, String functionName) {
    Long functionId =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper -> mapper.selectFunctionIdBySchemaIdAndFunctionName(schemaId, functionName));

    if (functionId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          functionName);
    }

    return functionId;
  }

  private void fillFunctionPOBuilderParentEntityId(FunctionPO.Builder builder, Namespace ns) {
    NamespaceUtil.checkFunction(ns);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionPOByVersionedIdentifier")
  FunctionPO getFunctionPOByVersionedIdentifier(NameIdentifier ident) {
    // Versioned ident format: namespace = [metalake, catalog, schema, functionName], name = version
    // Extract function namespace (metalake.catalog.schema), function name and version
    Namespace functionNamespace = NameIdentifier.of(ident.namespace().levels()).namespace();
    String functionName = NameIdentifier.of(ident.namespace().levels()).name();
    int version = Integer.parseInt(ident.name());

    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(functionNamespace.levels()), Entity.EntityType.SCHEMA);

    // Use join query with version parameter (-1 means latest version)
    FunctionPO functionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper ->
                mapper.selectFunctionMetaWithVersionBySchemaIdAndNameAndVersion(
                    schemaId, functionName, version));

    if (functionPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          ident.toString());
    }
    return functionPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateFunction")
  public <E extends Entity & HasIdentifier> FunctionEntity updateFunction(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifierUtil.checkFunction(identifier);

    // Build versioned identifier for getting latest version
    NameIdentifier latestVersionIdent =
        NameIdentifier.of(
            identifier.namespace().level(0),
            identifier.namespace().level(1),
            identifier.namespace().level(2),
            identifier.name(),
            String.valueOf(LATEST_VERSION));

    FunctionPO oldFunctionPO = getFunctionPOByVersionedIdentifier(latestVersionIdent);
    FunctionEntity oldFunctionEntity = fromFunctionPO(oldFunctionPO, identifier.namespace());
    FunctionEntity newEntity = (FunctionEntity) updater.apply((E) oldFunctionEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFunctionEntity.id(), newEntity.id()),
        "The updated function entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldFunctionEntity.id());

    try {
      // Check if version changed (need new version record)
      boolean versionChanged = newEntity.version() > oldFunctionEntity.version();

      FunctionPO newFunctionPO = updateFunctionPO(oldFunctionPO, newEntity);
      if (versionChanged) {
        // Insert new version and update function meta
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    FunctionVersionMetaMapper.class,
                    mapper ->
                        mapper.insertFunctionVersionMeta(newFunctionPO.getFunctionVersionPO())),
            () ->
                SessionUtils.doWithoutCommit(
                    FunctionMetaMapper.class,
                    mapper -> mapper.updateFunctionMeta(newFunctionPO, oldFunctionPO)));
      } else {
        // Just update function meta
        Integer updateResult =
            SessionUtils.doWithCommitAndFetchResult(
                FunctionMetaMapper.class,
                mapper -> mapper.updateFunctionMeta(newFunctionPO, oldFunctionPO));

        if (updateResult <= 0) {
          throw new IOException("Failed to update the entity: " + identifier);
        }
      }

      return newEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FUNCTION, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  // ============================ PO Converters ============================

  /**
   * Converts a FunctionPO (with embedded FunctionVersionPO) to a FunctionEntity.
   *
   * @param functionPO The function PO containing function metadata and version info.
   * @param namespace The namespace for the function.
   * @return The FunctionEntity.
   */
  FunctionEntity fromFunctionPO(FunctionPO functionPO, Namespace namespace) {
    try {
      FunctionVersionPO versionPO = functionPO.getFunctionVersionPO();
      List<FunctionDefinitionDTO> definitionDTOs =
          JsonUtils.anyFieldMapper()
              .readValue(
                  versionPO.getDefinitions(),
                  JsonUtils.anyFieldMapper()
                      .getTypeFactory()
                      .constructCollectionType(List.class, FunctionDefinitionDTO.class));
      FunctionDefinition[] definitions =
          definitionDTOs.stream()
              .map(FunctionDefinitionDTO::toFunctionDefinition)
              .toArray(FunctionDefinition[]::new);
      return FunctionEntity.builder()
          .withId(functionPO.getFunctionId())
          .withName(functionPO.getFunctionName())
          .withNamespace(namespace)
          .withComment(versionPO.getFunctionComment())
          .withFunctionType(FunctionType.valueOf(functionPO.getFunctionType()))
          .withDeterministic(
              functionPO.getDeterministic() != null && functionPO.getDeterministic() == 1)
          .withReturnType(
              JsonUtils.anyFieldMapper().readValue(functionPO.getReturnType(), Type.class))
          .withDefinitions(definitions)
          .withVersion(versionPO.getFunctionVersion())
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(functionPO.getAuditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize json object:", e);
    }
  }

  /**
   * Initializes a FunctionPO from a FunctionEntity.
   *
   * @param functionEntity The function entity.
   * @param builder The builder with parent entity IDs already set.
   * @return The initialized FunctionPO.
   */
  FunctionPO initializeFunctionPO(FunctionEntity functionEntity, FunctionPO.Builder builder) {
    try {
      FunctionVersionPO versionPO = initializeFunctionVersionPO(functionEntity, builder);
      return builder
          .withFunctionId(functionEntity.id())
          .withFunctionName(functionEntity.name())
          .withFunctionType(functionEntity.functionType().name())
          .withDeterministic(functionEntity.deterministic() ? 1 : 0)
          .withReturnType(
              JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.returnType()))
          .withFunctionVersionPO(versionPO)
          .withFunctionLatestVersion(functionEntity.version())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Initializes a FunctionVersionPO from a FunctionEntity.
   *
   * @param functionEntity The function entity.
   * @param builder The builder with parent entity IDs already set.
   * @return The initialized FunctionVersionPO.
   */
  FunctionVersionPO initializeFunctionVersionPO(
      FunctionEntity functionEntity, FunctionPO.Builder builder) {
    try {
      List<FunctionDefinitionDTO> definitionDTOs =
          Arrays.stream(functionEntity.definitions())
              .map(FunctionDefinitionDTO::fromFunctionDefinition)
              .collect(Collectors.toList());
      return FunctionVersionPO.builder()
          .withFunctionId(functionEntity.id())
          .withMetalakeId(builder.getMetalakeId())
          .withCatalogId(builder.getCatalogId())
          .withSchemaId(builder.getSchemaId())
          .withFunctionVersion(functionEntity.version())
          .withFunctionComment(functionEntity.comment())
          .withDefinitions(JsonUtils.anyFieldMapper().writeValueAsString(definitionDTOs))
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(functionEntity.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }

  /**
   * Creates an updated FunctionPO from an old PO and a new FunctionEntity.
   *
   * @param oldFunctionPO The existing function PO.
   * @param newFunction The new function entity with updated values.
   * @return The updated FunctionPO.
   */
  FunctionPO updateFunctionPO(FunctionPO oldFunctionPO, FunctionEntity newFunction) {
    try {
      FunctionPO.Builder builder =
          FunctionPO.builder()
              .withMetalakeId(oldFunctionPO.getMetalakeId())
              .withCatalogId(oldFunctionPO.getCatalogId())
              .withSchemaId(oldFunctionPO.getSchemaId());
      FunctionVersionPO newVersionPO = initializeFunctionVersionPO(newFunction, builder);
      return FunctionPO.builder()
          .withFunctionId(newFunction.id())
          .withFunctionName(newFunction.name())
          .withMetalakeId(oldFunctionPO.getMetalakeId())
          .withCatalogId(oldFunctionPO.getCatalogId())
          .withSchemaId(oldFunctionPO.getSchemaId())
          .withFunctionType(newFunction.functionType().name())
          .withDeterministic(newFunction.deterministic() ? 1 : 0)
          .withReturnType(JsonUtils.anyFieldMapper().writeValueAsString(newFunction.returnType()))
          .withFunctionLatestVersion(newFunction.version())
          .withFunctionVersionPO(newVersionPO)
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newFunction.auditInfo()))
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize json object:", e);
    }
  }
}
