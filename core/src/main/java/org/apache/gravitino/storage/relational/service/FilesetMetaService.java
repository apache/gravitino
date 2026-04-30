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
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.FilesetMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
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

      // insert both fileset meta table and version table
      SessionUtils.doMultipleWithCommit(
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetMetaMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetMetaOnDuplicateKeyUpdate(po);
                    } else {
                      mapper.insertFilesetMeta(po);
                    }
                  }),
          () ->
              SessionUtils.doWithoutCommit(
                  FilesetVersionMapper.class,
                  mapper -> {
                    if (overwrite) {
                      mapper.insertFilesetVersionsOnDuplicateKeyUpdate(po.getFilesetVersionPOs());
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
        "The updated fileset entity id: %s should be same with the table entity id before: %s",
        newEntity.id(),
        oldFilesetEntity.id());

    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String oldFullName = catalogName + "." + schemaName + "." + oldFilesetEntity.name();
    boolean isRenamed = !Objects.equals(oldFilesetEntity.name(), newEntity.name());

    Integer updateResult;
    try {
      boolean checkNeedUpdateVersion =
          POConverters.checkFilesetVersionNeedUpdate(
              oldFilesetPO.getFilesetVersionPOs(), newEntity);
      FilesetPO newFilesetPO =
          POConverters.updateFilesetPOWithVersion(oldFilesetPO, newEntity, checkNeedUpdateVersion);
      if (checkNeedUpdateVersion) {
        // These operations are performed atomically within a single transaction. The version
        // insert is protected by a unique constraint on `fileset_id + version + deleted_at`. If
        // the meta update affects 0 rows (concurrent modification), the transaction is rolled
        // back — including the version insert — and the update is treated as a conflict.
        int[] metaUpdateCountRef = new int[1];
        try {
          SessionUtils.doMultipleWithCommit(
              () ->
                  SessionUtils.doWithoutCommit(
                      FilesetVersionMapper.class,
                      mapper -> mapper.insertFilesetVersions(newFilesetPO.getFilesetVersionPOs())),
              () -> {
                metaUpdateCountRef[0] =
                    SessionUtils.getWithoutCommit(
                        FilesetMetaMapper.class,
                        mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO));
                if (metaUpdateCountRef[0] == 0) {
                  throw new RuntimeException("Failed to update the entity: " + identifier);
                }
              },
              () -> {
                if (isRenamed && metaUpdateCountRef[0] > 0) {
                  long now = System.currentTimeMillis();
                  SessionUtils.doWithoutCommit(
                      EntityChangeLogMapper.class,
                      mapper ->
                          mapper.insertChange(
                              metalakeName,
                              Entity.EntityType.FILESET.name(),
                              oldFullName,
                              OperateType.ALTER,
                              now));
                }
              });
          updateResult = 1;
        } catch (RuntimeException re) {
          if (metaUpdateCountRef[0] == 0) {
            // The meta update matched no rows; the transaction was rolled back,
            // including the version insert above.
            throw new IOException("Failed to update the entity: " + identifier);
          } else {
            ExceptionUtils.checkSQLException(
                re, Entity.EntityType.FILESET, newEntity.nameIdentifier().toString());
            throw re;
          }
        }
      } else {
        int[] metaUpdateCountRef = new int[1];
        SessionUtils.doMultipleWithCommit(
            () ->
                metaUpdateCountRef[0] =
                    SessionUtils.getWithoutCommit(
                        FilesetMetaMapper.class,
                        mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO)),
            () -> {
              if (isRenamed && metaUpdateCountRef[0] > 0) {
                long now = System.currentTimeMillis();
                SessionUtils.doWithoutCommit(
                    EntityChangeLogMapper.class,
                    mapper ->
                        mapper.insertChange(
                            metalakeName,
                            Entity.EntityType.FILESET.name(),
                            oldFullName,
                            OperateType.ALTER,
                            now));
              }
            });
        updateResult = metaUpdateCountRef[0];
      }
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLException(
          re, Entity.EntityType.FILESET, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteFileset")
  public boolean deleteFileset(NameIdentifier identifier) {
    FilesetPO filesetPO = getFilesetPOByIdentifier(identifier);
    Long filesetId = filesetPO.getFilesetId();

    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String filesetFullName = catalogName + "." + schemaName + "." + identifier.name();

    // We should delete meta and version info
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                FilesetMetaMapper.class,
                mapper -> mapper.softDeleteFilesetMetasByFilesetId(filesetId)),
        () ->
            SessionUtils.doWithoutCommit(
                FilesetVersionMapper.class,
                mapper -> mapper.softDeleteFilesetVersionsByFilesetId(filesetId)),
        () ->
            SessionUtils.doWithoutCommit(
                OwnerMetaMapper.class,
                mapper ->
                    mapper.softDeleteOwnerRelByMetadataObjectIdAndType(
                        filesetId, MetadataObject.Type.FILESET.name())),
        () ->
            SessionUtils.doWithoutCommit(
                SecurableObjectMapper.class,
                mapper ->
                    mapper.softDeleteObjectRelsByMetadataObject(
                        filesetId, MetadataObject.Type.FILESET.name())),
        () ->
            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeleteTagMetadataObjectRelsByMetadataObject(
                        filesetId, MetadataObject.Type.FILESET.name())),
        () ->
            SessionUtils.doWithoutCommit(
                StatisticMetaMapper.class,
                mapper -> mapper.softDeleteStatisticsByEntityId(filesetId)),
        () ->
            SessionUtils.doWithoutCommit(
                PolicyMetadataObjectRelMapper.class,
                mapper ->
                    mapper.softDeletePolicyMetadataObjectRelsByMetadataObject(
                        filesetId, MetadataObject.Type.FILESET.name())),
        () -> {
          long now = System.currentTimeMillis();
          SessionUtils.doWithoutCommit(
              EntityChangeLogMapper.class,
              mapper ->
                  mapper.insertChange(
                      metalakeName,
                      Entity.EntityType.FILESET.name(),
                      filesetFullName,
                      OperateType.DROP,
                      now));
        });

    return true;
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
}
