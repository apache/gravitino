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

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;
import static org.apache.gravitino.storage.relational.po.FunctionPO.buildFunctionPO;
import static org.apache.gravitino.storage.relational.po.FunctionPO.fromFunctionPO;
import static org.apache.gravitino.storage.relational.po.FunctionPO.initializeFunctionPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
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
  private BasePOStorageOps<FunctionPO, FunctionMetaMapper> ops;

  public static FunctionMetaService getInstance() {
    return INSTANCE;
  }

  private FunctionMetaService() {
    this.ops = new HierarchicalConversionPOStorageOps<>(new FunctionPOStorageOps());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listFunctionsByNamespace")
  public List<FunctionEntity> listFunctionsByNamespace(Namespace ns) {
    NamespaceUtil.checkFunction(ns);

    List<FunctionPO> functionPOs = listFunctionPOs(ns);
    return functionPOs.stream().map(f -> fromFunctionPO(f, ns)).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionByIdentifier")
  public FunctionEntity getFunctionByIdentifier(NameIdentifier ident) {
    FunctionPO functionPO = getFunctionPOByIdentifier(ident);
    return fromFunctionPO(functionPO, ident.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionIdBySchemaIdAndFunctionName")
  public Long getFunctionIdBySchemaIdAndFunctionName(Long schemaId, String functionName) {
    FunctionPO functionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class, mapper -> ops.getPO(mapper, schemaId, functionName));

    if (functionPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          functionName);
    }
    return functionPO.functionId();
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertFunction")
  public void insertFunction(FunctionEntity functionEntity, boolean overwrite) throws IOException {
    NameIdentifierUtil.checkFunction(functionEntity.nameIdentifier());

    FunctionPO.FunctionPOBuilder builder = FunctionPO.builder();
    try {
      fillFunctionPOBuilderParentEntityId(builder, functionEntity.namespace());
      FunctionPO po = initializeFunctionPO(functionEntity, builder);

      SessionUtils.doMultipleWithCommit(
          // Hold the parent schema row until this transaction ends, so the function cannot be
          // written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      functionEntity.nameIdentifier(),
                      po.schemaId(),
                      po.catalogId(),
                      po.metalakeId()),
          () -> insertFunctionWithoutCommit(functionEntity, po, overwrite));
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
    FunctionPO functionPO = getFunctionPOByIdentifier(ident);

    deleteFunctionWithVersion(ident, functionPO);
    return true;
  }

  /**
   * Deletes the observed function and its dependent rows in one transaction.
   *
   * <p>Package-private access lets concurrency tests submit a deliberately stale snapshot while
   * exercising the same root-first ordering as the public delete path.
   */
  void deleteFunctionWithVersion(NameIdentifier identifier, FunctionPO observedFunctionPO) {
    SessionUtils.doMultipleWithCommit(
        // Check the root version before touching relationships. A stale drop stops here.
        () ->
            OccWriteSupport.deleteWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        FunctionMetaMapper.class,
                        mapper ->
                            mapper.softDeleteFunctionMetaByFunctionId(
                                observedFunctionPO.functionId(),
                                observedFunctionPO.functionCurrentVersion())),
                () -> functionWriteFailure(identifier, observedFunctionPO)),
        () -> deleteFunctionDependents(observedFunctionPO.functionId()));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFunctionMetasByLegacyTimeline")
  public int deleteFunctionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int functionVersionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FunctionVersionMetaMapper.class,
            mapper -> mapper.deleteFunctionVersionMetasByLegacyTimeline(legacyTimeline, limit));

    int functionMetaDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FunctionMetaMapper.class,
            mapper -> mapper.deleteFunctionMetasByLegacyTimeline(legacyTimeline, limit));

    return functionVersionDeletedCount + functionMetaDeletedCount;
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
      long versionRetentionLine = functionCurVersion.version() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              FunctionVersionMetaMapper.class,
              mapper ->
                  mapper.softDeleteFunctionVersionsByRetentionLine(
                      functionCurVersion.functionId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      LOG.info(
          "Soft delete functionVersions count: {} which versions are older than or equal to"
              + " versionRetentionLine: {}, the current functionId and version is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          functionCurVersion.functionId(),
          functionCurVersion.version());
    }
    return totalDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFunctionPOByIdentifier")
  FunctionPO getFunctionPOByIdentifier(NameIdentifier ident) {
    NameIdentifierUtil.checkFunction(ident);
    FunctionPO functionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper -> POStorageReadRouting.getPO(mapper, ident, ops, Entity.EntityType.FUNCTION));

    if (functionPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FUNCTION.name().toLowerCase(Locale.ROOT),
          ident.name());
    }
    return functionPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateFunction")
  public <E extends Entity & HasIdentifier> FunctionEntity updateFunction(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    FunctionPO oldFunctionPO = getFunctionPOByIdentifier(identifier);
    FunctionEntity oldFunctionEntity = fromFunctionPO(oldFunctionPO, identifier.namespace());
    FunctionEntity newEntity = (FunctionEntity) updater.apply((E) oldFunctionEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFunctionEntity.id(), newEntity.id()),
        "The updated function entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldFunctionEntity.id());

    boolean isSchemaChanged = !newEntity.namespace().equals(oldFunctionEntity.namespace());
    NamespacedEntityId targetSchemaIds =
        isSchemaChanged
            ? EntityIdService.getEntityIds(
                NameIdentifier.of(newEntity.namespace().levels()), Entity.EntityType.SCHEMA)
            : null;
    Long newSchemaId = isSchemaChanged ? targetSchemaIds.entityId() : oldFunctionPO.schemaId();
    Long newCatalogId =
        isSchemaChanged ? targetSchemaIds.namespaceIds()[1] : oldFunctionPO.catalogId();
    Long newMetalakeId =
        isSchemaChanged ? targetSchemaIds.namespaceIds()[0] : oldFunctionPO.metalakeId();

    try {
      FunctionPO newFunctionPO =
          updateFunctionPO(oldFunctionPO, newEntity, newSchemaId, newCatalogId, newMetalakeId);
      SessionUtils.doMultipleWithCommit(
          () -> {
            if (isSchemaChanged) {
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      newEntity.nameIdentifier(), newSchemaId, newCatalogId, newMetalakeId);
            }
          },
          () -> {
            // function_current_version is the sole OCC token. The root CAS is the transaction's
            // decision point and must run before the unguarded version-row insert below.
            int updated =
                SessionUtils.getWithoutCommit(
                    FunctionMetaMapper.class,
                    mapper -> ops.updatePO(mapper, newFunctionPO, oldFunctionPO));
            if (updated == 0) {
              throw functionWriteFailure(identifier, oldFunctionPO);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  FunctionVersionMetaMapper.class,
                  mapper -> mapper.insertFunctionVersionMeta(newFunctionPO.functionVersionPO())));

      return newEntity;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FUNCTION, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public BasePOStorageOps<FunctionPO, FunctionMetaMapper> ops() {
    return ops;
  }

  private List<FunctionPO> listFunctionPOs(Namespace namespace) {
    return SessionUtils.getWithoutCommit(
        FunctionMetaMapper.class,
        mapper -> POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.FUNCTION));
  }

  private void fillFunctionPOBuilderParentEntityId(
      FunctionPO.FunctionPOBuilder builder, Namespace ns) {
    NamespaceUtil.checkFunction(ns);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(NameIdentifier.of(ns.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  private FunctionPO updateFunctionPO(
      FunctionPO oldFunctionPO,
      FunctionEntity newFunction,
      Long newSchemaId,
      Long newCatalogId,
      Long newMetalakeId) {
    // Both version columns always advance together, but deriving the next version from
    // the higher of the two keeps it above every version row that exists, even if a
    // future path ever leaves the current version behind the latest one.
    Integer newVersion = nextVersion(oldFunctionPO);
    FunctionPO.FunctionPOBuilder builder =
        FunctionPO.builder()
            .withMetalakeId(newMetalakeId)
            .withCatalogId(newCatalogId)
            .withSchemaId(newSchemaId)
            .withFunctionLatestVersion(newVersion)
            .withFunctionCurrentVersion(newVersion);
    return buildFunctionPO(newFunction, builder, newVersion);
  }

  /**
   * Writes a new function or replaces the active function selected by natural key or stable ID.
   * Locking the root before choosing the next version makes overwrite behavior identical across
   * databases and keeps standard reads strict about the matching version-row invariant.
   */
  private void insertFunctionWithoutCommit(
      FunctionEntity functionEntity, FunctionPO initializedFunctionPO, boolean overwrite) {
    if (!overwrite) {
      insertNewFunctionWithoutCommit(initializedFunctionPO);
      return;
    }

    FunctionPO existingFunctionPO = findAndLockFunctionForOverwrite(initializedFunctionPO);
    if (existingFunctionPO == null) {
      insertNewFunctionWithoutCommit(initializedFunctionPO);
      return;
    }

    FunctionPO replacementPO = functionPOForOverwrite(initializedFunctionPO, existingFunctionPO);
    int updated =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper -> ops.updatePO(mapper, replacementPO, existingFunctionPO));
    if (updated == 0) {
      throw functionWriteFailure(functionEntity.nameIdentifier(), existingFunctionPO);
    }
    SessionUtils.doWithoutCommit(
        FunctionVersionMetaMapper.class,
        mapper -> mapper.insertFunctionVersionMeta(replacementPO.functionVersionPO()));
  }

  private void insertNewFunctionWithoutCommit(FunctionPO functionPO) {
    SessionUtils.doWithoutCommit(
        FunctionMetaMapper.class, mapper -> ops.insertPO(mapper, functionPO, false));
    SessionUtils.doWithoutCommit(
        FunctionVersionMetaMapper.class,
        mapper -> mapper.insertFunctionVersionMeta(functionPO.functionVersionPO()));
  }

  private FunctionPO findAndLockFunctionForOverwrite(FunctionPO initializedFunctionPO) {
    FunctionPO sameNameFunctionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper ->
                mapper.selectFunctionMetaBySchemaIdAndNameForUpdate(
                    initializedFunctionPO.schemaId(), initializedFunctionPO.functionName()));
    if (sameNameFunctionPO != null) {
      return sameNameFunctionPO;
    }

    FunctionPO sameIdFunctionPO =
        SessionUtils.getWithoutCommit(
            FunctionMetaMapper.class,
            mapper -> mapper.selectFunctionMetaByIdForUpdate(initializedFunctionPO.functionId()));
    // Only a function that already lives under the target schema can be replaced by ID. A stored
    // function with the same ID under another schema belongs to a different parent: adopting it
    // here would move it without ever fencing its own schema.
    if (sameIdFunctionPO == null
        || !Objects.equals(sameIdFunctionPO.schemaId(), initializedFunctionPO.schemaId())) {
      return null;
    }
    return sameIdFunctionPO;
  }

  /**
   * Returns the version to write next, one above every version this function has ever had.
   *
   * @param functionPO the function row observed by the caller
   * @return the next monotonic version
   */
  private static int nextVersion(FunctionPO functionPO) {
    return Math.max(functionPO.functionCurrentVersion(), functionPO.functionLatestVersion()) + 1;
  }

  private FunctionPO functionPOForOverwrite(FunctionPO incomingPO, FunctionPO persistedPO) {
    int nextVersion = nextVersion(persistedPO);
    FunctionVersionPO incomingVersionPO = incomingPO.functionVersionPO();
    FunctionVersionPO persistedVersionPO =
        FunctionVersionPO.builder()
            .withFunctionId(persistedPO.functionId())
            .withMetalakeId(incomingVersionPO.metalakeId())
            .withCatalogId(incomingVersionPO.catalogId())
            .withSchemaId(incomingVersionPO.schemaId())
            .withFunctionVersion(nextVersion)
            .withFunctionComment(incomingVersionPO.functionComment())
            .withDefinitions(incomingVersionPO.definitions())
            .withAuditInfo(incomingVersionPO.auditInfo())
            .withDeletedAt(incomingVersionPO.deletedAt())
            .build();
    return FunctionPO.builder()
        .withFunctionId(persistedPO.functionId())
        .withFunctionName(incomingPO.functionName())
        .withMetalakeId(incomingPO.metalakeId())
        .withCatalogId(incomingPO.catalogId())
        .withSchemaId(incomingPO.schemaId())
        .withFunctionType(incomingPO.functionType())
        .withDeterministic(incomingPO.deterministic())
        .withFunctionLatestVersion(nextVersion)
        .withFunctionCurrentVersion(nextVersion)
        .withAuditInfo(incomingPO.auditInfo())
        .withDeletedAt(incomingPO.deletedAt())
        .withFunctionVersionPO(persistedVersionPO)
        .build();
  }

  private void deleteFunctionDependents(Long functionId) {
    SessionUtils.doWithoutCommit(
        FunctionVersionMetaMapper.class,
        mapper -> mapper.softDeleteFunctionVersionsByFunctionId(functionId));
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                functionId, MetadataObject.Type.FUNCTION.name()));
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(
                functionId, MetadataObject.Type.FUNCTION.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                functionId, MetadataObject.Type.FUNCTION.name()));
  }

  private RuntimeException functionWriteFailure(
      NameIdentifier identifier, FunctionPO observedFunctionPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.FUNCTION,
        () ->
            SessionUtils.getWithoutCommit(
                FunctionMetaMapper.class,
                mapper -> mapper.selectFunctionMetaByIdForUpdate(observedFunctionPO.functionId())),
        null,
        current ->
            Objects.equals(current.functionName(), observedFunctionPO.functionName())
                && Objects.equals(current.schemaId(), observedFunctionPO.schemaId())
                && Objects.equals(current.catalogId(), observedFunctionPO.catalogId())
                && Objects.equals(current.metalakeId(), observedFunctionPO.metalakeId()));
  }
}
