/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relation;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public interface RelationBackend extends Closeable {

  void initialize(Config config) throws IOException;

  <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Entity.EntityType entityType)
      throws IOException;

  boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException;

  <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException;

  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException;

  boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException;
}
