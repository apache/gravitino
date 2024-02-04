/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relation;

import static com.datastrato.gravitino.Configs.ENTITY_RELATION_STORE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.storage.relation.mysql.MySQLBackend;
import com.datastrato.gravitino.utils.Executable;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Relation store to store entities. This means we can store entities in a relational store. I.e.,
 * MySQL, PostgreSQL, etc. If you want to use a different backend, you can implement the {@link
 * RelationBackend} interface
 */
public class RelationEntityStore implements EntityStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationEntityStore.class);
  public static final ImmutableMap<String, String> RELATION_BACKENDS =
      ImmutableMap.of(Configs.DEFAULT_ENTITY_RELATION_STORE, MySQLBackend.class.getCanonicalName());
  private RelationBackend backend;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createRelationEntityBackend(config);
  }

  private static RelationBackend createRelationEntityBackend(Config config) {
    String backendName = config.get(ENTITY_RELATION_STORE);
    String className = RELATION_BACKENDS.getOrDefault(backendName, backendName);
    if (Objects.isNull(className)) {
      throw new RuntimeException("Unsupported backend type..." + backendName);
    }

    try {
      RelationBackend relationBackend =
          (RelationBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      relationBackend.initialize(config);
      return relationBackend;
    } catch (Exception e) {
      LOGGER.error("Failed to create and initialize RelationBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize RelationBackend by name: " + backendName, e);
    }
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    throw new UnsupportedOperationException("Unsupported operation in relation entity store.");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, Entity.EntityType entityType) throws IOException {
    return backend.list(namespace, entityType);
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    return backend.exists(ident, entityType);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    backend.insert(e, overwritten);
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    return backend.update(ident, entityType, updater);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    E entity = backend.get(ident, entityType);
    if (entity == null) {
      throw new NoSuchEntityException(ident.toString());
    }
    return entity;
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    return backend.delete(ident, entityType, cascade);
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) {
    throw new UnsupportedOperationException("Unsupported operation in relation entity store.");
  }

  @Override
  public void close() throws IOException {
    backend.close();
  }
}
