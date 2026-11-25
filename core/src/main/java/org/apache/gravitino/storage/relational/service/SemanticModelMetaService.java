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
import static org.apache.gravitino.semantic.SemanticModelRevisionSelector.ACTIVE;
import static org.apache.gravitino.semantic.SemanticModelRevisionSelector.LATEST;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.buildSemanticModelPO;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.fromSemanticModelPO;
import static org.apache.gravitino.storage.relational.po.SemanticModelPO.initializeSemanticModelPO;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelRevisionException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.semantic.SemanticModelRevisionSelector;
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogWriter;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelSourceMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
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
  private static final int METRIC_INSERT_BATCH_SIZE = 1000;
  private static final int SOURCE_INSERT_BATCH_SIZE = 1000;
  private static final SemanticModelMetaService INSTANCE = new SemanticModelMetaService();

  private final BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> activeOps =
      opsFor(ACTIVE);
  private final BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> latestOps =
      opsFor(LATEST);
  private final BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> identityOps =
      opsFor(null);

  /** Returns the singleton Semantic Model metadata service. */
  public static SemanticModelMetaService getInstance() {
    return INSTANCE;
  }

  private SemanticModelMetaService() {}

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
    Long id =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper -> mapper.selectSemanticModelIdBySchemaIdAndName(schemaId, semanticModelName));
    if (id == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
          semanticModelName);
    }
    return id;
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
    return listSemanticModelPOs(namespace, activeOps).stream()
        .filter(po -> po.getActiveVersion() > 0)
        .map(
            po -> {
              requireSnapshot(po, po.getActiveVersion(), "active");
              return fromSemanticModelPO(po, namespace);
            })
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
    return getSemanticModelByIdentifier(identifier, ACTIVE);
  }

  /** Loads the selected revision of an existing Semantic Model. */
  public SemanticModelEntity getSemanticModelByIdentifier(
      NameIdentifier identifier, SemanticModelRevisionSelector selector) {
    SemanticModelPO po =
        selectModel(
            identifier,
            selector == ACTIVE ? activeOps : selector == LATEST ? latestOps : opsFor(selector));
    if (selector == ACTIVE) {
      if (po.getActiveVersion() == 0) {
        throw missingModel(identifier);
      }
      requireSnapshot(po, po.getActiveVersion(), "active");
    } else if (selector == LATEST) {
      requireSnapshot(po, po.getCurrentVersion(), "latest");
    } else if (po.getSemanticModelVersionInfoPO() == null) {
      throw missingRevision(identifier, selector.getNumber(), po.getCurrentVersion());
    }
    return fromSemanticModelPO(po, identifier.namespace());
  }

  /** Checks for a live identity, including a model with only drafts. */
  public boolean semanticModelExists(NameIdentifier identifier) {
    try {
      selectModel(identifier, identityOps);
      return true;
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  /** Lists model identities under a schema for governance operations. */
  public List<NameIdentifier> listSemanticModelIdentifiers(Namespace namespace) {
    NamespaceUtil.checkSemanticModel(namespace);
    return listSemanticModelPOs(namespace, identityOps).stream()
        .map(po -> NameIdentifier.of(namespace, po.getSemanticModelName()))
        .toList();
  }

  /**
   * Inserts or overwrites a Semantic Model, its snapshot, metric identities, and supplied source
   * associations atomically.
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
      SchemaMetaService.getInstance()
          .doWithSchemaWriteLock(
              semanticModelEntity.nameIdentifier(),
              po.getSchemaId(),
              po.getCatalogId(),
              po.getMetalakeId(),
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
                          // Keep missing-row imports strict. A losing concurrent insert must not
                          // overwrite a persisted identity with a different ID.
                          mapper.insertSemanticModelMeta(po);
                          return;
                        }

                        SemanticModelPO replacementPO = semanticModelForOverwrite(po, storedPO);
                        Integer updated = mapper.updateSemanticModelMeta(replacementPO, storedPO);
                        if (updated == null || updated != 1) {
                          throw semanticModelWriteFailure(
                              semanticModelEntity.nameIdentifier(), storedPO);
                        }
                        persistedPO.set(replacementPO);
                      }),
              () ->
                  reconcileMetricIdentities(
                      persistedPO.get(), metricNames(semanticModelEntity.definition())),
              () -> replaceSources(persistedPO.get(), semanticModelEntity),
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
   * @throws NoSuchEntityException If the Semantic Model is deleted or renamed during the update.
   * @throws OptimisticLockException If the internal transaction loses a concurrent update race.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateSemanticModel")
  public <E extends Entity & HasIdentifier> SemanticModelEntity updateSemanticModel(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    SemanticModelPO observed = getSemanticModelPOByIdentifier(identifier);
    SemanticModelEntity oldEntity = fromSemanticModelPO(observed, identifier.namespace());
    SemanticModelEntity newEntity = (SemanticModelEntity) updater.apply((E) oldEntity);
    Preconditions.checkArgument(
        oldEntity.id().equals(newEntity.id()), "Semantic Model ID cannot change");
    Preconditions.checkArgument(
        oldEntity.namespace().equals(newEntity.namespace()),
        "Semantic Model namespace cannot change from %s to %s",
        oldEntity.namespace(),
        newEntity.namespace());

    try {
      return SessionUtils.doWithCommitAndFetchResult(
          SemanticModelMetaMapper.class,
          mapper -> {
            lockParent(identifier, observed);
            SemanticModelPO next = updateSemanticModelPO(observed, newEntity);
            if (mapper.updateSemanticModelMeta(next, observed) != 1) {
              throw semanticModelWriteFailure(identifier, observed);
            }
            if (!Objects.equals(
                    observed.getSemanticModelVersionInfoPO().metrics(),
                    next.getSemanticModelVersionInfoPO().metrics())
                && !metricNames(oldEntity.definition())
                    .equals(metricNames(newEntity.definition()))) {
              reconcileMetricIdentities(next, metricNames(newEntity.definition()));
            }
            replaceSources(next, newEntity);
            SessionUtils.doWithoutCommit(
                SemanticModelVersionInfoMapper.class,
                versions ->
                    versions.insertSemanticModelVersionInfo(next.getSemanticModelVersionInfoPO()));
            if (!observed.getSemanticModelName().equals(next.getSemanticModelName())) {
              EntityChangeLogWriter.append(
                  identifier, Entity.EntityType.SEMANTIC_MODEL, OperateType.ALTER);
            }
            return fromSemanticModelPO(next, identifier.namespace());
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(
          e, Entity.EntityType.SEMANTIC_MODEL, newEntity.nameIdentifier().toString());
      throw e;
    }
  }

  /**
   * Soft-deletes a Semantic Model, its metric identities, source associations, and version
   * snapshots.
   *
   * @param identifier The Semantic Model identifier.
   * @return {@code true} when an active identity was deleted.
   * @throws NoSuchEntityException If the Semantic Model is already deleted or renamed.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSemanticModel")
  public boolean deleteSemanticModel(NameIdentifier identifier) {
    SemanticModelPO semanticModelPO = selectModel(identifier, identityOps);
    deleteObservedSemanticModel(identifier, semanticModelPO);
    return true;
  }

  /**
   * Permanently deletes retired source associations, metric identities, and deleted models and
   * snapshots older than a timeline.
   *
   * @param legacyTimeline The exclusive deletion timeline in epoch milliseconds.
   * @param limit The maximum rows to delete from each table.
   * @return The total number of deleted rows.
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteSemanticModelMetasByLegacyTimeline")
  public int deleteSemanticModelMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int sourceDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelSourceMapper.class,
            mapper -> mapper.deleteByLegacyTimeline(legacyTimeline, limit));
    int metricDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelMetricMapper.class,
            mapper -> mapper.deleteSemanticModelMetricsByLegacyTimeline(legacyTimeline, limit));
    int versionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelVersionInfoMapper.class,
            mapper -> mapper.deleteSemanticModelVersionsByLegacyTimeline(legacyTimeline, limit));
    int metaDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            SemanticModelMetaMapper.class,
            mapper -> mapper.deleteSemanticModelMetasByLegacyTimeline(legacyTimeline, limit));
    return sourceDeletedCount + metricDeletedCount + versionDeletedCount + metaDeletedCount;
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
    for (SemanticModelVersionInfoPO candidate : currentVersions) {
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              SemanticModelMetaMapper.class,
              mapper -> {
                SemanticModelPO model =
                    mapper.selectSemanticModelMetaByIdForUpdate(candidate.semanticModelId());
                if (model == null) {
                  return 0;
                }
                long cutoff = model.getCurrentVersion() - versionRetentionCount;
                int count =
                    SessionUtils.getWithoutCommit(
                        SemanticModelVersionInfoMapper.class,
                        snapshots ->
                            snapshots.softDeleteSemanticModelVersionsByRetentionLine(
                                model.getSemanticModelId(),
                                cutoff,
                                model.getActiveVersion(),
                                limit));
                SessionUtils.doWithoutCommit(
                    SemanticModelSourceMapper.class,
                    sources -> sources.softDeleteUnreferencedRanges(model.getSemanticModelId()));
                return count;
              });
      totalDeletedCount += deletedCount;
      LOG.debug(
          "Soft-deleted {} revisions for Semantic Model {}",
          deletedCount,
          candidate.semanticModelId());
    }
    return totalDeletedCount;
  }

  /** Returns the persistent-object operations used by this service. */
  public BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops() {
    return activeOps;
  }

  /**
   * Deletes the observed Semantic Model and its snapshots in one transaction.
   *
   * <p>Package-private access lets concurrency tests submit an observed identity through the same
   * name guard as public deletion.
   */
  void deleteObservedSemanticModel(
      NameIdentifier identifier, SemanticModelPO observedSemanticModelPO) {
    Long semanticModelId = observedSemanticModelPO.getSemanticModelId();
    String metalakeName = identifier.namespace().level(0);
    NameIdentifier semanticModelIdent =
        NameIdentifierUtil.ofSemanticModel(
            metalakeName,
            identifier.namespace().level(1),
            identifier.namespace().level(2),
            observedSemanticModelPO.getSemanticModelName());
    // Orphaned owner, tag, and securable-object relations are cleaned up by
    // OrphanedMetadataObjectRelationService during garbage collection.
    SessionUtils.doMultipleWithCommit(
        () ->
            OccWriteSupport.deleteWithVersion(
                () ->
                    SessionUtils.getWithoutCommit(
                        SemanticModelMetaMapper.class,
                        mapper ->
                            mapper.softDeleteSemanticModelMetasBySemanticModelId(
                                semanticModelId, observedSemanticModelPO.getSemanticModelName())),
                () -> semanticModelWriteFailure(identifier, observedSemanticModelPO)),
        () ->
            SessionUtils.doWithoutCommit(
                SemanticModelVersionInfoMapper.class,
                mapper -> mapper.softDeleteSemanticModelVersionsBySemanticModelId(semanticModelId)),
        () ->
            SessionUtils.doWithoutCommit(
                SemanticModelMetricMapper.class,
                mapper -> mapper.softDeleteSemanticModelMetricsBySemanticModelId(semanticModelId)),
        () ->
            SessionUtils.doWithoutCommit(
                SemanticModelSourceMapper.class,
                mapper -> mapper.softDeleteByModelId(semanticModelId)),
        () ->
            EntityChangeLogWriter.append(
                semanticModelIdent, Entity.EntityType.SEMANTIC_MODEL, OperateType.DROP));
  }

  private void replaceSources(SemanticModelPO po, SemanticModelEntity entity) {
    if (!entity.sourceUpdate().replacesSources()) {
      return;
    }
    entity.sourceUpdate().validate(entity.definition());
    SessionUtils.doWithoutCommit(
        SemanticModelSourceMapper.class,
        mapper -> {
          mapper.closeSourceRanges(po.getSemanticModelId(), po.getCurrentVersion());
          List<SemanticModelSource> sources = entity.sourceUpdate().sources();
          for (int start = 0; start < sources.size(); start += SOURCE_INSERT_BATCH_SIZE) {
            mapper.insertSources(
                po.getMetalakeId(),
                po.getSemanticModelId(),
                po.getCurrentVersion(),
                sources.subList(start, Math.min(start + SOURCE_INSERT_BATCH_SIZE, sources.size())));
          }
        });
  }

  private static void reconcileMetricIdentities(SemanticModelPO model, Set<String> names) {
    // The caller has already inserted or version-checked the model in this transaction. A locking
    // read also observes the latest identities on backends using repeatable-read isolation.
    SessionUtils.doWithoutCommit(
        SemanticModelMetricMapper.class,
        mapper -> {
          Map<String, SemanticModelMetricPO> live =
              mapper
                  .selectSemanticModelMetricsByModelIdForUpdate(model.getSemanticModelId())
                  .stream()
                  .collect(
                      Collectors.toMap(SemanticModelMetricPO::metricName, Function.identity()));
          List<String> addedNames = new ArrayList<>();
          for (String name : names) {
            if (live.remove(name) == null) {
              addedNames.add(name);
            }
          }
          if (!addedNames.isEmpty()) {
            IdGenerator generator = GravitinoEnv.getInstance().idGenerator();
            Preconditions.checkState(
                generator != null, "Metric identity ID generator is not initialized");
            List<SemanticModelMetricPO> addedMetrics = new ArrayList<>(addedNames.size());
            for (String name : addedNames) {
              addedMetrics.add(
                  SemanticModelMetricPO.builder()
                      .withMetricId(generator.nextId())
                      .withMetalakeId(model.getMetalakeId())
                      .withCatalogId(model.getCatalogId())
                      .withSchemaId(model.getSchemaId())
                      .withSemanticModelId(model.getSemanticModelId())
                      .withMetricName(name)
                      .withDeletedAt(0L)
                      .build());
            }
            Lists.partition(addedMetrics, METRIC_INSERT_BATCH_SIZE)
                .forEach(mapper::insertSemanticModelMetrics);
          }
          if (!live.isEmpty()) {
            mapper.softDeleteSemanticModelMetricsByIds(
                model.getSemanticModelId(),
                live.values().stream()
                    .map(SemanticModelMetricPO::metricId)
                    .collect(Collectors.toList()));
          }
        });
  }

  private static Set<String> metricNames(SemanticModelDefinition definition) {
    Set<String> names = new LinkedHashSet<>();
    Metric[] metrics = definition.metrics();
    if (metrics != null) {
      for (Metric metric : metrics) {
        Preconditions.checkArgument(
            names.add(metric.name()), "Duplicate metric name: %s", metric.name());
      }
    }
    return names;
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
        .withActiveVersion(nextVersion)
        .withLastVersion(nextVersion)
        .withDeletedAt(source.getDeletedAt())
        .withSemanticModelVersionInfoPO(
            versionInfoForOverwrite(
                source.getSemanticModelVersionInfoPO(), persistedPO, nextVersion))
        .build();
  }

  private static SemanticModelVersionInfoPO versionInfoForOverwrite(
      SemanticModelVersionInfoPO source, SemanticModelPO persistedPO, int nextVersion) {
    return source.toBuilder()
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withSemanticModelId(persistedPO.getSemanticModelId())
        .withVersion(nextVersion)
        .build();
  }

  SemanticModelPO getSemanticModelPOByIdentifier(NameIdentifier identifier) {
    SemanticModelPO po = selectModel(identifier, latestOps);
    requireSnapshot(po, po.getCurrentVersion(), "latest");
    return po;
  }

  private static void lockParent(NameIdentifier identifier, SemanticModelPO observed) {
    SchemaMetaService.getInstance()
        .lockSchemaForEntityWrite(
            identifier, observed.getSchemaId(), observed.getCatalogId(), observed.getMetalakeId());
  }

  private static void requireSnapshot(SemanticModelPO po, int revision, String pointer) {
    Preconditions.checkState(
        po.getSemanticModelVersionInfoPO() != null,
        "Missing %s snapshot %s for Semantic Model %s",
        pointer,
        revision,
        po.getSemanticModelId());
  }

  private static NoSuchEntityException missingModel(NameIdentifier identifier) {
    return new NoSuchEntityException(
        NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
        Entity.EntityType.SEMANTIC_MODEL.name().toLowerCase(Locale.ROOT),
        identifier.name());
  }

  private static NoSuchSemanticModelRevisionException missingRevision(
      NameIdentifier identifier, int revision, int latest) {
    return revision <= latest
        ? new NoSuchSemanticModelRevisionException(
            "Revision %s of Semantic Model %s was removed by version retention",
            revision, identifier)
        : new NoSuchSemanticModelRevisionException(
            "Revision %s of Semantic Model %s does not exist", revision, identifier);
  }

  private SemanticModelPO selectModel(
      NameIdentifier identifier, BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops) {
    NameIdentifierUtil.checkSemanticModel(identifier);
    SemanticModelPO po =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper ->
                POStorageReadRouting.getPO(
                    mapper, identifier, ops, Entity.EntityType.SEMANTIC_MODEL));
    if (po == null) {
      throw missingModel(identifier);
    }
    return po;
  }

  private static BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> opsFor(
      @Nullable SemanticModelRevisionSelector selector) {
    return new HierarchicalConversionPOStorageOps<>(new SemanticModelPOStorageOps(selector));
  }

  private RuntimeException semanticModelWriteFailure(
      NameIdentifier identifier, SemanticModelPO observedSemanticModelPO) {
    return OccWriteSupport.writeFailure(
        identifier,
        Entity.EntityType.SEMANTIC_MODEL,
        () ->
            SessionUtils.getWithoutCommit(
                SemanticModelMetaMapper.class,
                mapper ->
                    mapper.selectSemanticModelMetaByIdForUpdate(
                        observedSemanticModelPO.getSemanticModelId())),
        null,
        current ->
            Objects.equals(
                    current.getSemanticModelName(), observedSemanticModelPO.getSemanticModelName())
                && Objects.equals(current.getSchemaId(), observedSemanticModelPO.getSchemaId())
                && Objects.equals(current.getCatalogId(), observedSemanticModelPO.getCatalogId())
                && Objects.equals(
                    current.getMetalakeId(), observedSemanticModelPO.getMetalakeId()));
  }

  private List<SemanticModelPO> listSemanticModelPOs(
      Namespace namespace, BasePOStorageOps<SemanticModelPO, SemanticModelMetaMapper> ops) {
    return SessionUtils.getWithoutCommit(
        SemanticModelMetaMapper.class,
        mapper ->
            POStorageReadRouting.listPOs(mapper, namespace, ops, Entity.EntityType.SEMANTIC_MODEL));
  }
}
