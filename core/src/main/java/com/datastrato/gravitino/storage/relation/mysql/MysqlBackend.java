/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relation.mysql;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.storage.relation.RelationBackend;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class MysqlBackend implements RelationBackend {
  @Override
  public void initialize(Config config) throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Unsupported operation now.");
  }
}
