/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import java.io.Closeable;
import java.io.IOException;

public interface EntityStore extends Closeable {

  /**
   * Initialize the entity store.
   *
   * <p>Note. This method will be called after the EntityStore object is created, and before any
   * other methods are called.
   *
   * @param config the configuration for the entity store
   * @throws RuntimeException if the initialization fails
   */
  void initialize(Config config) throws RuntimeException;

  /**
   * Store the entity into the underlying storage.
   *
   * <p>Note. The implementation should be thread-safe, and should be able to handle concurrent
   * store of entities.
   *
   * @param entity the entity to store
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   */
  <E extends Entity & HasIdentifier> void storeEntity(E entity) throws IOException;

  /**
   * Retrieve the entity from the underlying storage.
   *
   * <p>Note. The implementation should be thread-safe, and should be able to handle concurrent
   * retrieve of entities.
   *
   * @param ident the unique identifier of the entity
   * @return the entity retrieved from the underlying storage
   * @param <E> the type of the entity
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the retrieve operation fails
   */
  <E extends Entity & HasIdentifier> E retrieveEntity(HasIdentifier ident)
      throws NoSuchEntityException, IOException;
}
