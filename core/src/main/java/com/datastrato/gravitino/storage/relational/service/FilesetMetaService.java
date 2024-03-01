/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.po.FilesetPO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
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

  public FilesetEntity getFilesetByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkFileset(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          identifier.namespace().toString());
    }

    FilesetPO filesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetMetaBySchemaIdAndName(schemaId, identifier.name()));

    if (filesetPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          identifier.toString());
    }

    return POConverters.fromFilesetPO(filesetPO, identifier.namespace());
  }

  public List<FilesetEntity> listFilesetsByNamespace(Namespace namespace) {
    Namespace.checkFileset(namespace);
    String metalakeName = namespace.level(0);
    String catalogName = namespace.level(1);
    String schemaName = namespace.level(2);

    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          namespace.toString());
    }

    List<FilesetPO> filesetPOs =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class, mapper -> mapper.listFilesetPOsBySchemaId(schemaId));
    return POConverters.fromFilesetPOs(filesetPOs, namespace);
  }

  public void insertFileset(FilesetEntity filesetEntity, boolean overwrite) {
    try {
      NameIdentifier.checkFileset(filesetEntity.nameIdentifier());
      String metalakeName = filesetEntity.namespace().level(0);
      String catalogName = filesetEntity.namespace().level(1);
      String schemaName = filesetEntity.namespace().level(2);
      Long metalakeId =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
      if (metalakeId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.METALAKE.name().toLowerCase(),
            metalakeName);
      }

      Long catalogId =
          SessionUtils.getWithoutCommit(
              CatalogMetaMapper.class,
              mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
      if (catalogId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.CATALOG.name().toLowerCase(),
            String.format("%s.%s", metalakeName, catalogName));
      }

      Long schemaId =
          SessionUtils.getWithoutCommit(
              SchemaMetaMapper.class,
              mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
      if (schemaId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
            Entity.EntityType.SCHEMA.name().toLowerCase(),
            filesetEntity.namespace().toString());
      }

      FilesetPO po =
          POConverters.initializeFilesetPOWithVersion(
              filesetEntity, metalakeId, catalogId, schemaId);

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
      if (re.getCause() != null
          && re.getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Fileset entity: %s already exists", filesetEntity.nameIdentifier()));
      }
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> FilesetEntity updateFileset(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkFileset(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String filesetName = identifier.name();

    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          identifier.namespace().toString());
    }

    FilesetPO oldFilesetPO =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetMetaBySchemaIdAndName(schemaId, filesetName));
    if (oldFilesetPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.FILESET.name().toLowerCase(),
          identifier.toString());
    }

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
          POConverters.checkFilesetVersionNeedUpdate(
              oldFilesetPO.getFilesetVersionPO(),
              newEntity,
              metalakeId,
              catalogId,
              schemaId,
              oldFilesetPO.getFilesetId());
      FilesetPO newFilesetPO =
          POConverters.updateFilesetPOWithVersion(
              oldFilesetPO, newEntity, metalakeId, catalogId, schemaId, checkNeedUpdateVersion);
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
      if (re.getCause() != null
          && re.getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Table entity: %s already exists", newEntity.nameIdentifier()));
      }
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
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.namespace().level(1);
    String schemaName = identifier.namespace().level(2);
    String filesetName = identifier.name();
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }

    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.CATALOG.name().toLowerCase(),
          String.format("%s.%s", metalakeName, catalogName));
    }

    Long schemaId =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.selectSchemaIdByCatalogIdAndName(catalogId, schemaName));
    if (schemaId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.SCHEMA.name().toLowerCase(),
          identifier.namespace().toString());
    }

    Long filesetId =
        SessionUtils.getWithoutCommit(
            FilesetMetaMapper.class,
            mapper -> mapper.selectFilesetIdBySchemaIdAndName(schemaId, filesetName));
    if (filesetId != null) {
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
    }
    return true;
  }
}
