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
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.buildSemanticModelPO;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.fromSemanticModelPO;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.initializeSemanticModelPO;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides relational persistence operations for Semantic Model metadata. */
public class SemanticModelMetaService {

  private static final Logger LOG = LoggerFactory.getLogger(SemanticModelMetaService.class);
  private static final SemanticModelMetaService INSTANCE = new SemanticModelMetaService();

  private final BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops;

  /** Returns the singleton Semantic Model metadata service. */
  public static SemanticModelMetaService getInstance() {
    return INSTANCE;
  }

  private SemanticModelMetaService() {
    this.ops = new HierarchicalConversionPOStorageOps<>(new SemanticModelPOStorageOps());
  }

  /**
   * Resolves a Semantic Model stable ID by schema ID and name.
   *
   * @param schemaId The parent schema ID.
   * @param semanticModelName The Semantic Model name.
   * @return The stable Semantic Model ID.
   * @throws NoSuchEntityException If the Semantic Model does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSemanticModelIdBySchemaIdAndName")
  public Long getSemanticModelIdBySchemaIdAndName(long schemaId, String semanticModelName) {
    SemanticModelPO semanticModelPO =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper -> ops.getPO(mapper, schemaId, semanticModelName));
    if (semanticModelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
          semanticModelName);
    }
    return semanticModelPO.getSemanticModelId();
  }

  /**
   * Lists current Semantic Models under a namespace.
   *
   * @param namespace The Semantic Model namespace.
   * @return The current Semantic Model entities.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listSemanticModelsByNamespace")
  public List<SemanticModelEntity> listSemanticModelsByNamespace(Namespace namespace) {
    NamespaceUtil.checkSemanticModel(namespace);
    return listSemanticModelPOs(namespace).stream()
        .map(po -> fromSemanticModelPO(po, namespace))
        .collect(Collectors.toList());
  }

  /**
   * Loads the current Semantic Model by identifier.
   *
   * @param identifier The Semantic Model identifier.
   * @return The current Semantic Model entity.
   * @throws NoSuchEntityException If the Semantic Model does not exist.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getSemanticModelByIdentifier")
  public SemanticModelEntity getSemanticModelByIdentifier(NameIdentifier identifier) {
    SemanticModelPO semanticModelPO = getSemanticModelPOByIdentifier(identifier);
    return fromSemanticModelPO(semanticModelPO, identifier.namespace());
  }

  /**
   * Inserts or overwrites a Semantic Model identity and its snapshot atomically.
   *
   * @param semanticModelEntity The Semantic Model entity.
   * @param overwrite Whether to overwrite rows for the same stable ID or natural key.
   * @throws IOException If relational persistence fails.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertSemanticModel")
  public void insertSemanticModel(SemanticModelEntity semanticModelEntity, boolean overwrite)
      throws IOException {
    NameIdentifierUtil.checkSemanticModel(semanticModelEntity.nameIdentifier());
    try {
      SemanticModelPO po =
          initializeSemanticModelPO(semanticModelEntity, SemanticModelPO.builder());
      AtomicReference<SemanticModelPO> persistedPO = new AtomicReference<>(po);
      SessionUtils.doMultipleWithCommit(
          // Hold the observed parent schema row until this transaction ends, so a Semantic Model
          // cannot be written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      semanticModelEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  SemanticModelMetaMapper.class,
                  mapper -> {
                    SemanticModelPO storedPO =
                        overwrite
                            ? mapper.selectSemanticModelMetaBySchemaIdAndNameForUpdate(
                                po.getSchemaId(), po.getSemanticModelName())
                            : null;
                    if (storedPO == null) {
                      // Keep a missing-row import strict. Concurrent imports that both observed no
                      // identity must not turn the losing insert into an overwrite with another ID.
                      ops.insertPO(mapper, po, false);
                      return;
                    }

                    SemanticModelPO replacementPO = semanticModelForOverwrite(po, storedPO);
                    Integer updated = mapper.updateSemanticModelMeta(replacementPO, storedPO);
                    Preconditions.checkState(
                        updated != null && updated == 1,
                        "The overwritten Semantic Model %s in schema %s changed while its row was held",
                        po.getSemanticModelName(),
                        po.getSchemaId());
                    persistedPO.set(replacementPO);
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  SemanticModelVersionInfoMapper.class,
                  mapper ->
                      mapper.insertSemanticModelVersionInfo(
                          persistedPO.get().getSemanticModelVersionInfoPO())));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SEMANTIC_MODEL, semanticModelEntity.nameIdentifier().toString());
      throw re;
    }
  }

  /**
   * Atomically creates a complete new snapshot and advances the current version pointer.
   *
   * @param identifier The current Semantic Model identifier.
   * @param updater The entity updater.
   * @param <E> The internal entity type accepted by the updater.
   * @return The updated Semantic Model entity.
   * @throws IOException If persistence fails.
   * @throws OptimisticLockException If the internal transaction loses a concurrent update race.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateSemanticModel")
  public <E extends Entity & HasIdentifier> SemanticModelEntity updateSemanticModel(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    SemanticModelPO oldSemanticModelPO = getSemanticModelPOByIdentifier(identifier);
    SemanticModelEntity oldSemanticModelEntity =
        fromSemanticModelPO(oldSemanticModelPO, identifier.namespace());
    SemanticModelEntity newEntity = (SemanticModelEntity) updater.apply((E) oldSemanticModelEntity);
    Preconditions.checkArgument(
        Objects.equals(oldSemanticModelEntity.id(), newEntity.id()),
        "The updated Semantic Model entity id: %s should be same with the entity id before: %s",
        newEntity.id(),
        oldSemanticModelEntity.id());

    AtomicInteger updateResult = new AtomicInteger(-1);
    try {
      SemanticModelPO newSemanticModelPO = updateSemanticModelPO(oldSemanticModelPO, newEntity);
      String metalakeName = identifier.namespace().level(0);
      String catalogName = identifier.namespace().level(1);
      String schemaName = identifier.namespace().level(2);
      String oldFullName =
          NameIdentifierUtil.ofSemanticModel(
                  metalakeName, catalogName, schemaName, oldSemanticModelPO.getSemanticModelName())
              .toString();
      boolean isRenamed =
          !Objects.equals(
              oldSemanticModelPO.getSemanticModelName(), newSemanticModelPO.getSemanticModelName());

      SessionUtils.doMultipleWithCommit(
          // The Semantic Model and its parent were read before this transaction started. Lock the
          // observed parent again before either identity or version writes, so a schema cascade
          // cannot finish its cleanup and then let this update recreate child state below it.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      identifier,
                      oldSemanticModelPO.getSchemaId(),
                      oldSemanticModelPO.getCatalogId(),
                      oldSemanticModelPO.getMetalakeId()),
          () -> {
            updateResult.set(
                SessionUtils.getWithoutCommit(
                    SemanticModelMetaMapper.class,
                    mapper -> ops.updatePO(mapper, newSemanticModelPO, oldSemanticModelPO)));
            if (updateResult.get() == 0) {
              throw new OptimisticLockException(
                  "Semantic Model %s changed during the internal update transaction", identifier);
            }
          },
          () ->
              SessionUtils.doWithoutCommit(
                  SemanticModelVersionInfoMapper.class,
                  mapper ->
                      mapper.insertSemanticModelVersionInfo(
                          newSemanticModelPO.getSemanticModelVersionInfoPO())),
          () -> {
            if (isRenamed && updateResult.get() > 0) {
              SessionUtils.doWithoutCommit(
                  EntityChangeLogMapper.class,
                  mapper ->
                      mapper.insertEntityChange(
                          metalakeName,
                          Entity.EntityType.SEMANTIC_MODEL.name(),
                          oldFullName,
                          OperateType.ALTER));
            }
          });
      return newEntity;
    } catch (OptimisticLockException optimisticLockException) {
      throw optimisticLockException;
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.SEMANTIC_MODEL, newEntity.nameIdentifier().toString());
      throw re;
    }
  }

  /**
   * Soft-deletes a Semantic Model identity and all of its version snapshots.
   *
   * @param identifier The Semantic Model identifier.
   * @return {@code true} when an active identity was deleted.
   * @throws OptimisticLockException If the internal transaction loses a concurrent update race.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSemanticModel")
  public boolean deleteSemanticModel(NameIdentifier identifier) {
    SemanticModelPO semanticModelPO = getSemanticModelPOByIdentifier(identifier);
    String metalakeName = identifier.namespace().level(0);
    String fullName =
        NameIdentifierUtil.ofSemanticModel(
                metalakeName,
                identifier.namespace().level(1),
                identifier.namespace().level(2),
                semanticModelPO.getSemanticModelName())
            .toString();
    return deleteSemanticModel(
        semanticModelPO.getSemanticModelId(),
        semanticModelPO.getCurrentVersion(),
        metalakeName,
        fullName);
  }

  /**
   * Permanently deletes soft-deleted Semantic Model identities and snapshots older than a timeline.
   *
   * @param legacyTimeline The exclusive deletion timeline in epoch milliseconds.
   * @param limit The maximum rows to delete from each table.
   * @return The total number of deleted rows.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSemanticModelMetasByLegacyTimeline")
  public int deleteSemanticModelMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int versionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelVersionInfoMapper.class,
            mapper -> mapper.deleteSemanticModelVersionsByLegacyTimeline(legacyTimeline, limit));
    int metaDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelMetaMapper.class,
            mapper -> mapper.deleteSemanticModelMetasByLegacyTimeline(legacyTimeline, limit));
    return versionDeletedCount + metaDeletedCount;
  }

  /**
   * Soft-deletes old Semantic Model snapshots beyond the configured retention count.
   *
   * @param versionRetentionCount The number of latest versions to retain per Semantic Model.
   * @param limit The maximum versions to delete for each Semantic Model.
   * @return The number of snapshots soft-deleted.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSemanticModelVersionsByRetentionCount")
  public int deleteSemanticModelVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    List<SemanticModelVersionInfoPO> currentVersions =
        SessionUtils.getWithoutCommit(
            SemanticModelVersionInfoMapper.class,
            mapper -> mapper.selectSemanticModelVersionsByRetentionCount(versionRetentionCount));

    int totalDeletedCount = 0;
    for (SemanticModelVersionInfoPO currentVersion : currentVersions) {
      long versionRetentionLine = currentVersion.version() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              SemanticModelVersionInfoMapper.class,
              mapper ->
                  mapper.softDeleteSemanticModelVersionsByRetentionLine(
                      currentVersion.semanticModelId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;
      LOG.info(
          "Soft delete Semantic Model versions count: {} through retention line: {},"
              + " current Semantic Model id and version: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          currentVersion.semanticModelId(),
          currentVersion.version());
    }
    return totalDeletedCount;
  }

  /** Returns the persistent-object operations used by this service. */
  public BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops() {
    return ops;
  }

  boolean deleteSemanticModel(
      Long semanticModelId,
      Integer expectedCurrentVersion,
      String metalakeName,
      String semanticModelFullName) {
    AtomicInteger deleteResult = new AtomicInteger();
    SessionUtils.doMultipleWithCommit(
        () -> {
          deleteResult.set(
              SessionUtils.getWithoutCommit(
                  SemanticModelMetaMapper.class,
                  mapper ->
                      mapper.softDeleteSemanticModelMetasBySemanticModelId(
                          semanticModelId, expectedCurrentVersion)));
          if (deleteResult.get() == 0) {
            throw new OptimisticLockException(
                "Semantic Model %s changed during the internal drop transaction",
                semanticModelFullName);
          }
        },
        () -> {
          if (deleteResult.get() > 0) {
            SessionUtils.doWithoutCommit(
                SemanticModelVersionInfoMapper.class,
                mapper -> mapper.softDeleteSemanticModelVersionsBySemanticModelId(semanticModelId));
          }
        },
        () -> {
          if (deleteResult.get() > 0) {
            SessionUtils.doWithoutCommit(
                EntityChangeLogMapper.class,
                mapper ->
                    mapper.insertEntityChange(
                        metalakeName,
                        Entity.EntityType.SEMANTIC_MODEL.name(),
                        semanticModelFullName,
                        OperateType.DROP));
          }
        });
    return deleteResult.get() > 0;
  }

  private SemanticModelPO updateSemanticModelPO(
      SemanticModelPO oldSemanticModelPO, SemanticModelEntity newEntity) {
    int previousVersion =
        Math.max(oldSemanticModelPO.getCurrentVersion(), oldSemanticModelPO.getLastVersion());
    Preconditions.checkState(
        previousVersion < Integer.MAX_VALUE,
        "Semantic Model %s has exhausted the version range",
        oldSemanticModelPO.getSemanticModelId());
    int newVersion = previousVersion + 1;
    SemanticModelPO.SemanticModelPOBuilder builder =
        SemanticModelPO.builder()
            .withMetalakeId(oldSemanticModelPO.getMetalakeId())
            .withCatalogId(oldSemanticModelPO.getCatalogId())
            .withSchemaId(oldSemanticModelPO.getSchemaId())
            .withCurrentVersion(newVersion)
            .withLastVersion(newVersion);
    return buildSemanticModelPO(newEntity, builder, newVersion);
  }

  private static SemanticModelPO semanticModelForOverwrite(
      SemanticModelPO source, SemanticModelPO persistedPO) {
    int previousVersion = Math.max(persistedPO.getCurrentVersion(), persistedPO.getLastVersion());
    Preconditions.checkState(
        previousVersion < Integer.MAX_VALUE,
        "Semantic Model %s has exhausted the version range",
        persistedPO.getSemanticModelId());
    int nextVersion = previousVersion + 1;
    return SemanticModelPO.builder()
        .withSemanticModelId(persistedPO.getSemanticModelId())
        .withSemanticModelName(source.getSemanticModelName())
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withAuditInfo(source.getAuditInfo())
        .withCurrentVersion(nextVersion)
        .withLastVersion(nextVersion)
        .withDeletedAt(source.getDeletedAt())
        .withSemanticModelVersionInfoPO(
            versionInfoForOverwrite(
                source.getSemanticModelVersionInfoPO(), persistedPO, nextVersion))
        .build();
  }

  private static SemanticModelVersionInfoPO versionInfoForOverwrite(
      SemanticModelVersionInfoPO source, SemanticModelPO persistedPO, int nextVersion) {
    return SemanticModelVersionInfoPO.builder()
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withSemanticModelId(persistedPO.getSemanticModelId())
        .withVersion(nextVersion)
        .withSemanticModelName(source.semanticModelName())
        .withSemanticModelComment(source.semanticModelComment())
        .withSemanticModelDefinition(source.semanticModelDefinition())
        .withProperties(source.properties())
        .withAuditInfo(source.auditInfo())
        .withDeletedAt(source.deletedAt())
        .build();
  }

  private SemanticModelPO getSemanticModelPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkSemanticModel(identifier);
    SemanticModelPO semanticModelPO =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(
                    mapper, identifier, ops, Entity.EntityType.SEMANTIC_MODEL));
    if (semanticModelPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
          identifier.name());
    }
    return semanticModelPO;
  }

  private List<SemanticModelPO> listSemanticModelPOs(Namespace namespace) {
    return SessionUtils.getWithoutCommit(
        SemanticModelMetaMapper.class,
        mapper ->
            POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.SEMANTIC_MODEL));
  }
}
