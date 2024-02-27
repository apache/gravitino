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
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
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

  public BaseMetalake getMetalakeByIdentifier(NameIdentifier ident) {
    NameIdentifier.checkMetalake(ident);
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
    if (metalakePO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, ident.toString());
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
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Metalake entity: %s already exists", baseMetalake.nameIdentifier()));
      }
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
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, ident.toString());
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
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Catalog entity: %s already exists", newMetalakeEntity.nameIdentifier()));
      }
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
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(ident.name()));
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
            () -> {
              // TODO We will cascade delete the metadata of sub-resources under the metalake
            });
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
