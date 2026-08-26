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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.FilesetMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service class for fileset metadata and version info. It provides the basic database
 * operations for fileset and version info.
 */
public class FilesetMetaService {
  private static final FilesetMetaService INSTANCE = new FilesetMetaService();

  private static final Logger LOG = LoggerFactory.getLogger(FilesetMetaService.class);

  public static FilesetMetaService getInstance() {
    return INSTANCE;
  }

  private FilesetMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFilesetPOBySchemaIdAndName")
  public FilesetPO getFilesetPOBySchemaIdAndName(Long schemaId, String filesetName) {
    FilesetPO filesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetMetaBySchemaIdAndName(schemaId, filesetName));

    if (filesetPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          filesetName);
    }
    return filesetPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFilesetIdBySchemaIdAndName")
  public Long getFilesetIdBySchemaIdAndName(Long schemaId, String filesetName) {
    Long filesetId =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetIdBySchemaIdAndName(schemaId, filesetName));

    if (filesetId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          filesetName);
    }
    return filesetId;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getFilesetByIdentifier")
  public FilesetEntity getFilesetByIdentifier(NameIdentifier identifier) {
    FilesetPO filesetPO = getFilesetPOByIdentifier(identifier);
    return POConverters.fromFilesetPO(filesetPO, identifier.namespace());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listFilesetsByNamespace")
  public List<FilesetEntity> listFilesetsByNamespace(Namespace namespace) {
    NamespaceUtil.checkFileset(namespace);

    List<FilesetPO> filesetPOs = listFilesetPOs(namespace);
    return POConverters.fromFilesetPOs(filesetPOs, namespace);
  }

  private List<FilesetPO> listFilesetPOs(Namespace namespace) {
    return filesetListFetcher().apply(namespace);
  }

  private List<FilesetPO> listFilesetPOsBySchemaId(Namespace namespace) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    return SessionUtils.getWithoutCommit(
        FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsBySchemaId(schemaId));
  }

  private List<FilesetPO> listFilesetPOsByFullQualifiedName(Namespace namespace) {
    String[] namespaceLevels = namespace.levels();
    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper ->
                mapper.listFilesetPOsByFullQualifiedName(
                    namespaceLevels[0], namespaceLevels[1], namespaceLevels[2]));
    if (filesetPOs.isEmpty() || filesetPOs.get(0).getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }
    return filesetPOs.stream().filter(po -> po.getFilesetId() != null).collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertFileset")
  public void insertFileset(FilesetEntity filesetEntity, boolean overwrite) throws IOException {
    try {
      NameIdentifierUtil.checkFileset(filesetEntity.nameIdentifier());

      FilesetPO.Builder builder = FilesetPO.builder();
      fillFilesetPOBuilderParentEntityId(builder, filesetEntity.namespace());

      FilesetPO po = POConverters.initializeFilesetPOWithVersion(filesetEntity, builder);
      AtomicReference<FilesetPO> persistedPO = new AtomicReference<>(po);

      // The schema lock, metadata row, and every storage-location version row share one
      // transaction. A failure in any later step restores all earlier writes.
      SessionUtils.doMultipleWithCommit(
          // Hold the parent schema row until this transaction ends, so the fileset cannot be
          // written below a schema that is being dropped.
          () ->
              SchemaMetaService.getInstance()
                  .lockSchemaForEntityWrite(
                      filesetEntity.nameIdentifier(),
                      po.getSchemaId(),
                      po.getCatalogId(),
                      po.getMetalakeId()),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetMetaOnDuplicateKeyUpdate(po);
                      // MySQL/H2 can resolve the upsert through the natural key and preserve an
                      // existing fileset ID. The database also derives the next OCC version, so
                      // read both values back before building the dependent version rows.
                      FilesetPO storedPO =
                          mapper.selectFilesetMetaBySchemaIdAndNameForUpdate(
                              po.getSchemaId(), po.getFilesetName());
                      Preconditions.checkState(
                          storedPO != null,
                          "The overwritten fileset %s in schema %s does not exist",
                          po.getFilesetName(),
                          po.getSchemaId());
                      persistedPO.set(filesetPOWithPersistedIdentityAndVersion(po, storedPO));
                    } else {
                      mapper.insertFilesetMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetVersionsOnDuplicateKeyUpdate(
                          persistedPO.get().getFilesetVersionPOs());
                    } else {
                      mapper.insertFilesetVersions(po.getFilesetVersionPOs());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FILESET, filesetEntity.nameIdentifier().toString());
      throw re;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "updateFileset")
  public <E extends Entity & HasIdentifier> FilesetEntity updateFileset(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    FilesetPO oldFilesetPO = getFilesetPOByIdentifier(identifier);
    FilesetEntity oldFilesetEntity =
        POConverters.fromFilesetPO(oldFilesetPO, identifier.namespace());
    FilesetEntity newEntity = (FilesetEntity) updater.apply((E) oldFilesetEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFilesetEntity.id(), newEntity.id()),
        "The updated fileset entity id: %s should be same with the fileset entity id before: %s",
        newEntity.id(),
        oldFilesetEntity.id());

    try {
      FilesetPO newFilesetPO = POConverters.updateFilesetPOWithVersion(oldFilesetPO, newEntity);
      SessionUtils.doMultipleWithCommit(
          () -> {
            // Decide the winner before writing fileset_version_info. Two writers that read version
            // N both prepare version N + 1, but only one can change the metadata row. The loser
            // stops here, so it cannot overwrite any storage-location row written by the winner.
            int updated =
                SessionUtils.getWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO));
            if (updated == 0) {
              throw filesetWriteFailure(identifier, oldFilesetPO);
            }
          },
          () -> {
            // The metadata row now points to this complete snapshot. It stays in the same
            // transaction so a failed version insert also restores the metadata version.
            SessionUtils.doWithoutCommit(
                FilesetVersionMapper.class,
                mapper -> mapper.insertFilesetVersions(newFilesetPO.getFilesetVersionPOs()));
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FILESET, newEntity.nameIdentifier().toString());
      throw re;
    }

    return newEntity;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFileset")
  public boolean deleteFileset(NameIdentifier identifier) {
    FilesetPO filesetPO = getFilesetPOByIdentifier(identifier);

    // Delete the root row first and only if it still has the version we read. A stale drop stops
    // before it can remove versions, tags, policies, or any other related data.
    SessionUtils.doMultipleWithCommit(
        () -> deleteFilesetWithVersion(identifier, filesetPO),
        () -> deleteFilesetDependents(filesetPO.getFilesetId()));

    return true;
  }

  /**
   * Soft-deletes the observed fileset metadata row without starting a transaction.
   *
   * <p>The caller must run this method in the same transaction as dependent cleanup. Package access
   * also lets concurrency tests submit a deliberately stale snapshot without duplicating the
   * production CAS logic.
   *
   * @param identifier the fileset identity observed by the caller
   * @param observedFilesetPO the fileset row and OCC version observed by the caller
   */
  void deleteFilesetWithVersion(NameIdentifier identifier, FilesetPO observedFilesetPO) {
    int deleted =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper ->
                mapper.softDeleteFilesetMetasByFilesetId(
                    observedFilesetPO.getFilesetId(), observedFilesetPO.getCurrentVersion()));
    if (deleted == 0) {
      throw filesetWriteFailure(identifier, observedFilesetPO);
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFilesetAndVersionMetasByLegacyTimeline")
  public int deleteFilesetAndVersionMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    int filesetDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FilesetMetaMapper.class,
            mapper -> {
              return mapper.deleteFilesetMetasByLegacyTimeline(legacyTimeline, limit);
            });
    int filesetVersionDeletedCount =
        SessionUtils.doWithCommitAndFetchResult(
            FilesetVersionMapper.class,
            mapper -> {
              return mapper.deleteFilesetVersionsByLegacyTimeline(legacyTimeline, limit);
            });
    return filesetDeletedCount + filesetVersionDeletedCount;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFilesetVersionsByRetentionCount")
  public int deleteFilesetVersionsByRetentionCount(Long versionRetentionCount, int limit) {
    // get the current version of all filesets.
    List<FilesetMaxVersionPO> filesetCurVersions =
        SessionUtils.getWithoutCommit(
            FilesetVersionMapper.class,
            mapper -> mapper.selectFilesetVersionsByRetentionCount(versionRetentionCount));

    // soft delete old versions that are older than or equal to (currentVersion -
    // versionRetentionCount).
    int totalDeletedCount = 0;
    for (FilesetMaxVersionPO filesetCurVersion : filesetCurVersions) {
      long versionRetentionLine = filesetCurVersion.getVersion() - versionRetentionCount;
      int deletedCount =
          SessionUtils.doWithCommitAndFetchResult(
              FilesetVersionMapper.class,
              mapper ->
                  mapper.softDeleteFilesetVersionsByRetentionLine(
                      filesetCurVersion.getFilesetId(), versionRetentionLine, limit));
      totalDeletedCount += deletedCount;

      // log the deletion by current fileset version.
      LOG.info(
          "Soft delete filesetVersions count: {} which versions are older than or equal to"
              + " versionRetentionLine: {}, the current filesetId and version is: <{}, {}>.",
          deletedCount,
          versionRetentionLine,
          filesetCurVersion.getFilesetId(),
          filesetCurVersion.getVersion());
    }
    return totalDeletedCount;
  }

  private FilesetPO getFilesetPOByIdentifier(NameIdentifier identifier) {
    NameIdentifierUtil.checkFileset(identifier);

    return filesetPOFetcher().apply(identifier);
  }

  private FilesetPO getFilesetPOBySchemaId(NameIdentifier identifier) {
    Long schemaId =
        EntityIdService.getEntityId(
            NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);
    return getFilesetPOBySchemaIdAndName(schemaId, identifier.name());
  }

  private FilesetPO getFilesetPOByFullQualifiedName(NameIdentifier identifier) {
    String[] namespaceLevels = identifier.namespace().levels();
    FilesetPO filesetPO =
        getFilesetByFullQualifiedName(
            namespaceLevels[0], namespaceLevels[1], namespaceLevels[2], identifier.name());

    if (filesetPO.getSchemaId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespaceLevels[2]);
    }

    if (filesetPO.getFilesetId() == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          identifier.name());
    }

    return filesetPO;
  }

  private Function<Namespace, List<FilesetPO>> filesetListFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::listFilesetPOsBySchemaId
        : this::listFilesetPOsByFullQualifiedName;
  }

  private Function<NameIdentifier, FilesetPO> filesetPOFetcher() {
    return GravitinoEnv.getInstance().cacheEnabled()
        ? this::getFilesetPOBySchemaId
        : this::getFilesetPOByFullQualifiedName;
  }

  private void fillFilesetPOBuilderParentEntityId(FilesetPO.Builder builder, Namespace namespace) {
    NamespaceUtil.checkFileset(namespace);
    NamespacedEntityId namespacedEntityId =
        EntityIdService.getEntityIds(
            NameIdentifier.of(namespace.levels()), Entity.EntityType.SCHEMA);
    builder.withMetalakeId(namespacedEntityId.namespaceIds()[0]);
    builder.withCatalogId(namespacedEntityId.namespaceIds()[1]);
    builder.withSchemaId(namespacedEntityId.entityId());
  }

  private FilesetPO getFilesetByFullQualifiedName(
      String metalakeName, String catalogName, String schemaName, String filesetName) {
    FilesetPO filesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper ->
                mapper.selectFilesetByFullQualifiedName(
                    metalakeName, catalogName, schemaName, filesetName));
    if (filesetPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          filesetName);
    }

    return filesetPO;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetFilesetByIdentifier")
  public List<FilesetEntity> batchGetFilesetByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    NameIdentifier schemaIdent = NameIdentifierUtil.getSchemaIdentifier(firstIdent);
    List<String> filesetNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        FilesetMetaMapper.class,
        mapper -> {
          List<FilesetPO> filesetPOs =
              mapper.batchSelectFilesetByIdentifier(
                  schemaIdent.namespace().level(0),
                  schemaIdent.namespace().level(1),
                  schemaIdent.name(),
                  filesetNames);
          return POConverters.fromFilesetPOs(filesetPOs, firstIdent.namespace());
        });
  }

  private FilesetPO filesetPOWithPersistedIdentityAndVersion(
      FilesetPO incomingPO, FilesetPO persistedPO) {
    // The upsert chooses the version inside the database and may keep an existing fileset ID. All
    // storage-location rows must use those stored values or the metadata row would point at a
    // version snapshot that cannot be loaded.
    List<FilesetVersionPO> persistedVersions =
        incomingPO.getFilesetVersionPOs().stream()
            .map(
                versionPO ->
                    FilesetVersionPO.builder()
                        .withMetalakeId(persistedPO.getMetalakeId())
                        .withCatalogId(persistedPO.getCatalogId())
                        .withSchemaId(persistedPO.getSchemaId())
                        .withFilesetId(persistedPO.getFilesetId())
                        .withVersion(persistedPO.getCurrentVersion())
                        .withFilesetComment(versionPO.getFilesetComment())
                        .withProperties(versionPO.getProperties())
                        .withLocationName(versionPO.getLocationName())
                        .withStorageLocation(versionPO.getStorageLocation())
                        .withDeletedAt(versionPO.getDeletedAt())
                        .build())
            .collect(Collectors.toList());
    return FilesetPO.builder()
        .withFilesetId(persistedPO.getFilesetId())
        .withFilesetName(persistedPO.getFilesetName())
        .withMetalakeId(persistedPO.getMetalakeId())
        .withCatalogId(persistedPO.getCatalogId())
        .withSchemaId(persistedPO.getSchemaId())
        .withType(persistedPO.getType())
        .withAuditInfo(persistedPO.getAuditInfo())
        .withCurrentVersion(persistedPO.getCurrentVersion())
        .withLastVersion(persistedPO.getLastVersion())
        .withDeletedAt(persistedPO.getDeletedAt())
        .withFilesetVersionPOs(persistedVersions)
        .build();
  }

  private void deleteFilesetDependents(Long filesetId) {
    // The fileset row has already passed its version check. All cleanup below uses the same
    // transaction, so either the root and every related row are deleted together, or none are.
    SessionUtils.doWithoutCommit(
        FilesetVersionMapper.class,
        mapper -> mapper.softDeleteFilesetVersionsByFilesetId(filesetId));
    SessionUtils.doWithoutCommit(
        OwnerMetaMapper.class,
        mapper ->
            mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                filesetId, MetadataObject.Type.FILESET.name()));
    SessionUtils.doWithoutCommit(
        SecurableObjectMapper.class,
        mapper ->
            mapper.softDeleteObjectRelsByMetadataObject(
                filesetId, MetadataObject.Type.FILESET.name()));
    SessionUtils.doWithoutCommit(
        TagMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                filesetId, MetadataObject.Type.FILESET.name()));
    SessionUtils.doWithoutCommit(
        StatisticMetaMapper.class, mapper -> mapper.softDeleteStatisticsByEntityId(filesetId));
    SessionUtils.doWithoutCommit(
        PolicyMetadataObjectRelMapper.class,
        mapper ->
            mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                filesetId, MetadataObject.Type.FILESET.name()));
  }

  private RuntimeException filesetWriteFailure(
      NameIdentifier identifier, FilesetPO observedFilesetPO) {
    // A zero-row CAS means either another writer advanced this fileset, or the fileset disappeared
    // from the name the caller used. Locking the stable ID waits for an in-flight writer to commit,
    // so the result can be classified from committed identity data.
    FilesetPO currentFilesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetMetaByIdForUpdate(observedFilesetPO.getFilesetId()));
    if (currentFilesetPO == null
        || !Objects.equals(currentFilesetPO.getFilesetName(), observedFilesetPO.getFilesetName())
        || !Objects.equals(currentFilesetPO.getSchemaId(), observedFilesetPO.getSchemaId())
        || !Objects.equals(currentFilesetPO.getCatalogId(), observedFilesetPO.getCatalogId())
        || !Objects.equals(currentFilesetPO.getMetalakeId(), observedFilesetPO.getMetalakeId())) {
      return new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          identifier.name());
    }
    return ExceptionUtils.concurrentModification(Entity.EntityType.FILESET, identifier);
  }
}
