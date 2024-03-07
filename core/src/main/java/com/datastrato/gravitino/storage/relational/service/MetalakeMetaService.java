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
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.utils.ExceptionUtils;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The service class for metalake metadata. It provides the basic database operations for metalake.
 */
public class MetalakeMetaService {
  private static final MetalakeMetaService INSTANCE = new MetalakeMetaService();

  public static MetalakeMetaService getInstance() {
    return INSTANCE;
  }

  private MetalakeMetaService() {}

  public List<BaseMetalake> listMetalakes() {
    List<MetalakePO> metalakePOS =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, MetalakeMetaMapper::listMetalakePOs);
    return POConverters.fromMetalakePOs(metalakePOS);
  }

  public Long getMetalakeIdByName(String metalakeName) {
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          metalakeName);
    }
    return metalakeId;
  }

  public BaseMetalake getMetalakeByIdentifier(NameIdentifier ident) {
    NameIdentifier.checkMetalake(ident);
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }
    return POConverters.fromMetalakePO(metalakePO);
  }

  public void insertMetalake(BaseMetalake baseMetalake, boolean overwrite) {
    try {
      NameIdentifier.checkMetalake(baseMetalake.nameIdentifier());
      SessionUtils.doWithCommit(
          MetalakeMetaMapper.class,
          mapper -> {
            MetalakePO po = POConverters.initializeMetalakePOWithVersion(baseMetalake);
            if (overwrite) {
              mapper.insertMetalakeMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertMetalakeMeta(po);
            }
          });
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.METALAKE, baseMetalake.nameIdentifier().toString());
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> BaseMetalake updateMetalake(
      NameIdentifier ident, Function<E, E> updater) throws IOException {
    NameIdentifier.checkMetalake(ident);
    MetalakePO oldMetalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (oldMetalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.METALAKE.name().toLowerCase(),
          ident.toString());
    }

    BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
    BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
    Preconditions.checkArgument(
        Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
        "The updated metalake entity id: %s should be same with the metalake entity id before: %s",
        newMetalakeEntity.id(),
        oldMetalakeEntity.id());
    MetalakePO newMetalakePO =
        POConverters.updateMetalakePOWithVersion(oldMetalakePO, newMetalakeEntity);
    Integer updateResult;
    try {
      updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              MetalakeMetaMapper.class,
              mapper -> mapper.updateMetalakeMeta(newMetalakePO, oldMetalakePO));
    } catch (RuntimeException re) {
      ExceptionUtils.checkSQLConstraintException(
          re, Entity.EntityType.METALAKE, newMetalakeEntity.nameIdentifier().toString());
      throw re;
    }

    if (updateResult > 0) {
      return newMetalakeEntity;
    } else {
      throw new IOException("Failed to update the entity: " + ident);
    }
  }

  public boolean deleteMetalake(NameIdentifier ident, boolean cascade) {
    NameIdentifier.checkMetalake(ident);
    Long metalakeId = getMetalakeIdByName(ident.name());
    if (metalakeId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    MetalakeMetaMapper.class,
                    mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    CatalogMetaMapper.class,
                    mapper -> mapper.softDeleteCatalogMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    SchemaMetaMapper.class,
                    mapper -> mapper.softDeleteSchemaMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    TableMetaMapper.class,
                    mapper -> mapper.softDeleteTableMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetMetaMapper.class,
                    mapper -> mapper.softDeleteFilesetMetasByMetalakeId(metalakeId)),
            () ->
                SessionUtils.doWithoutCommit(
                    FilesetVersionMapper.class,
                    mapper -> mapper.softDeleteFilesetVersionsByMetalakeId(metalakeId)));
      } else {
        List<CatalogEntity> catalogEntities =
            CatalogMetaService.getInstance()
                .listCatalogsByNamespace(Namespace.ofCatalog(ident.name()));
        if (!catalogEntities.isEmpty()) {
          throw new NonEmptyEntityException(
              "Entity %s has sub-entities, you should remove sub-entities first", ident);
        }
        SessionUtils.doWithCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId));
      }
    }
    return true;
  }
}
