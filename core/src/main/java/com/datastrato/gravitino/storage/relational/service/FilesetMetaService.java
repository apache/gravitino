/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.po.FilesetPO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The service class for fileset metadata and version info. It provides the basic database
 * operations for fileset and version info.
 */
public class FilesetMetaService {
  private static final FilesetMetaService INSTANCE = new FilesetMetaService();

  public static FilesetMetaService getInstance() {
    return INSTANCE;
  }

  private FilesetMetaService() {}

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

  public FilesetEntity getFilesetByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    FilesetPO filesetPO = getFilesetPOBySchemaIdAndName(schemaId, filesetName);

    return POConverters.fromFilesetPO(filesetPO, identifier.namespace());
  }

  public List<FilesetEntity> listFilesetsByNamespace(Namespace namespace) {
    Namespace.checkFileset(namespace);

    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(namespace);

    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsBySchemaId(schemaId));

    return POConverters.fromFilesetPOs(filesetPOs, namespace);
  }

  public void insertFileset(FilesetEntity filesetEntity, boolean overwrite) {
    try {
      NameIdentifier.checkFileset(filesetEntity.nameIdentifier());

      FilesetPO.Builder builder = new FilesetPO.Builder();
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
                      mapper.insertFilesetVersionOnDuplicateKeyUpdate(po.getFilesetVersionPO());
                    } else {
                      mapper.insertFilesetVersion(po.getFilesetVersionPO());
                    }
                  }));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.FILESET, filesetEntity.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> FilesetEntity updateFileset(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    FilesetPO oldFilesetPO = getFilesetPOBySchemaIdAndName(schemaId, filesetName);
    FilesetEntity oldFilesetEntity =
        POConverters.fromFilesetPO(oldFilesetPO, identifier.namespace());
    FilesetEntity newEntity = (FilesetEntity) updater.apply((E) oldFilesetEntity);
    Preconditions.checkArgument(
        Objects.equals(oldFilesetEntity.id(), newEntity.id()),
        "The updated fileset entity id: %s should be same with the table entity id before: %s",
        newEntity.id(),
        oldFilesetEntity.id());

    Integer updateResult;
    try {
      boolean checkNeedUpdateVersion =
          POConverters.checkFilesetVersionNeedUpdate(oldFilesetPO.getFilesetVersionPO(), newEntity);
      FilesetPO newFilesetPO =
          POConverters.updateFilesetPOWithVersion(oldFilesetPO, newEntity, checkNeedUpdateVersion);
      if (checkNeedUpdateVersion) {
        // These operations are guaranteed to be atomic by the transaction. If version info is
        // inserted successfully and the uniqueness is guaranteed by `fileset_id + version +
        // deleted_at`, it means that no other transaction has been inserted (if a uniqueness
        // conflict occurs, the transaction will be rolled back), then we can consider that the
        // fileset meta update is successful
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.insertFilesetVersion(newFilesetPO.getFilesetVersionPO())),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO)));
        // we set the updateResult to 1 to indicate that the update is successful
        updateResult = 1;
      } else {
        updateResult =
            SessionUtils.doWithCommitAndFetchResult(
                FilesetMetaMapper.class,
                mapper -> mapper.updateFilesetMeta(newFilesetPO, oldFilesetPO));
      }
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.FILESET, newEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteFileset(NameIdentifier identifier) {
    NameIdentifier.checkFileset(identifier);

    String filesetName = identifier.name();

    Long schemaId =
        CommonMetaService.getInstance().getParentEntityIdByNamespace(identifier.namespace());

    Long filesetId = getFilesetIdBySchemaIdAndName(schemaId, filesetName);

    // We should delete meta and version info
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                FilesetMetaMapper.class,
                mapper -> mapper.softDeleteFilesetMetasByFilesetId(filesetId)),
        () ->
            SessionUtils.doWithoutCommit(
                FilesetVersionMapper.class,
                mapper -> mapper.softDeleteFilesetVersionsByFilesetId(filesetId)));

    return true;
  }

  private void fillFilesetPOBuilderParentEntityId(FilesetPO.Builder builder, Namespace namespace) {
    Namespace.checkFileset(namespace);
    Long parentEntityId = null;
    for (int level = 0; level < namespace.levels().length; level++) {
      String name = namespace.level(level);
      switch (level) {
        case 0:
          parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(name);
          builder.withMetalakeId(parentEntityId);
          continue;
        case 1:
          parentEntityId =
              CatalogMetaService.getInstance()
                  .getCatalogIdByMetalakeIdAndName(parentEntityId, name);
          builder.withCatalogId(parentEntityId);
          continue;
        case 2:
          parentEntityId =
              SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(parentEntityId, name);
          builder.withSchemaId(parentEntityId);
          break;
      }
    }
  }
}
