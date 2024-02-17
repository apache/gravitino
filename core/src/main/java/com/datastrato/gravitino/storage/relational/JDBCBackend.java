/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * {@link JDBCBackend} is a jdbc implementation of RelationalBackend interface. If we want to use
 * another relational implementation, We can just implement {@link RelationalBackend} interface and
 * use it in the Gravitino.
 */
public class JDBCBackend implements RelationalBackend {

  /** Initialize the jdbc backend instance. */
  @Override
  public void initialize(Config config) {
    SqlSessionFactoryHelper.getInstance().init(config);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) {
    if (entityType == Entity.EntityType.METALAKE) {
      List<MetalakePO> metalakePOS =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, MetalakeMetaMapper::listMetalakePOs);
      return (List<E>) POConverters.fromMetalakePOs(metalakePOS);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for list operation", entityType));
    }
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) {
    if (entityType == Entity.EntityType.METALAKE) {
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
      return metalakePO != null;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for exists operation", entityType));
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    if (e instanceof BaseMetalake) {
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class,
              mapper -> mapper.selectMetalakeMetaByName(e.nameIdentifier().name()));
      if (!overwritten && metalakePO != null) {
        throw new EntityAlreadyExistsException(
            String.format("Metalake entity: %s already exists", e.nameIdentifier().name()));
      }
      SessionUtils.doWithCommit(
          MetalakeMetaMapper.class,
          mapper -> {
            if (overwritten) {
              mapper.insertMetalakeMetaWithUpdate(POConverters.toMetalakePO((BaseMetalake) e));
            } else {
              mapper.insertMetalakeMeta(POConverters.toMetalakePO((BaseMetalake) e));
            }
          });
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for insert operation", e.getClass()));
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    if (entityType == Entity.EntityType.METALAKE) {
      MetalakePO oldMetalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));

      BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
      BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
      Preconditions.checkArgument(
          Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
          String.format(
              "The updated metalake entity id: %s should same with the metalake entity id before: %s",
              newMetalakeEntity.id(), oldMetalakeEntity.id()));

      Integer updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              MetalakeMetaMapper.class,
              mapper ->
                  mapper.updateMetalakeMeta(
                      POConverters.toMetalakePO(newMetalakeEntity), oldMetalakePO));
      if (updateResult > 0) {
        return (E) newMetalakeEntity;
      } else {
        throw new IOException("Failed to update the entity:" + ident);
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for update operation", entityType));
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException {
    if (entityType == Entity.EntityType.METALAKE) {
      MetalakePO metalakePO =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeMetaByName(ident.name()));
      if (metalakePO == null) {
        throw new NoSuchEntityException("No such entity:%s", ident.toString());
      }
      return (E) POConverters.fromMetalakePO(metalakePO);
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for get operation", entityType));
    }
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade) {
    if (entityType == Entity.EntityType.METALAKE) {
      Long metalakeId =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(ident.name()));
      if (metalakeId != null) {
        if (cascade) {
          SessionUtils.doMultipleWithCommit(
              () ->
                  SessionUtils.doWithoutCommit(
                      MetalakeMetaMapper.class,
                      mapper -> mapper.deleteMetalakeMetaById(metalakeId)),
              () -> {
                // TODO We will cascade delete the metadata of sub-resources under the metalake
              });
        } else {
          SessionUtils.doWithCommit(
              MetalakeMetaMapper.class, mapper -> mapper.deleteMetalakeMetaById(metalakeId));
        }
      }
      return true;
    } else {
      throw new IllegalArgumentException(
          String.format("Unsupported entity type: %s for delete operation", entityType));
    }
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
  }
}
