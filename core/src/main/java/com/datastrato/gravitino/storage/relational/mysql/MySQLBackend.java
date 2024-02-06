/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.mysql;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.storage.relational.RelationalBackend;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * {@link MySQLBackend} is a MySQL implementation of RelationalBackend interface. If we want to use
 * another relational implementation, We can just implement {@link RelationalBackend} interface and
 * use it in the Gravitino.
 */
public class MySQLBackend implements RelationalBackend {

  /** Initialize the MySQL backend instance. */
  @Override
  public void initialize(Config config) {}

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) throws NoSuchEntityException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws NoSuchEntityException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade) {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public void close() throws IOException {}
}
