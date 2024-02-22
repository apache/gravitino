/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.UnsupportedEntityTypeException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * {@link JDBCBackend} is a jdbc implementation of {@link RelationalBackend} interface. You can use
 * a database that supports the JDBC protocol as storage. If the specified database has special SQL
 * syntax, please implement the SQL statements and methods in MyBatis Mapper separately and switch
 * according to the {@link Configs#ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY} parameter.
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
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for list operation", entityType);
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
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for exists operation", entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    if (e instanceof BaseMetalake) {
      try {
        SessionUtils.doWithCommit(
            MetalakeMetaMapper.class,
            mapper -> {
              MetalakePO po =
                  POConverters.initializeMetalakePOVersion(
                      POConverters.toMetalakePO((BaseMetalake) e));
              if (overwritten) {
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
              String.format("Metalake entity: %s already exists", e.nameIdentifier().name()));
        }
        throw re;
      }
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", e.getClass());
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
      if (oldMetalakePO == null) {
        throw new NoSuchEntityException("No such entity:%s", ident.toString());
      }

      BaseMetalake oldMetalakeEntity = POConverters.fromMetalakePO(oldMetalakePO);
      BaseMetalake newMetalakeEntity = (BaseMetalake) updater.apply((E) oldMetalakeEntity);
      Preconditions.checkArgument(
          Objects.equals(oldMetalakeEntity.id(), newMetalakeEntity.id()),
          "The updated metalake entity id: %s should same with the metalake entity id before: %s",
          newMetalakeEntity.id(),
          oldMetalakeEntity.id());
      MetalakePO newMetalakePO =
          POConverters.updateMetalakePOVersion(
              oldMetalakePO, POConverters.toMetalakePO(newMetalakeEntity));

      Integer updateResult =
          SessionUtils.doWithCommitAndFetchResult(
              MetalakeMetaMapper.class,
              mapper -> mapper.updateMetalakeMeta(newMetalakePO, oldMetalakePO));
      if (updateResult > 0) {
        return (E) newMetalakeEntity;
      } else {
        throw new IOException("Failed to update the entity:" + ident);
      }
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for update operation", entityType);
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
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for get operation", entityType);
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
                      mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId)),
              () -> {
                // TODO We will cascade delete the metadata of sub-resources under the metalake
              });
        } else {
          // TODO Check whether the sub-resources are empty. If the sub-resources are not empty,
          //  deletion is not allowed.
          SessionUtils.doWithCommit(
              MetalakeMetaMapper.class,
              mapper -> mapper.softDeleteMetalakeMetaByMetalakeId(metalakeId));
        }
      }
      return true;
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for delete operation", entityType);
    }
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
  }
}
