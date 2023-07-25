/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.util.Executable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

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
   * Set the {@link EntitySerDe} for the entity store. {@link EntitySerDe} will be used to serialize
   * and deserialize the entities to the target format, and vice versa.
   *
   * @param entitySerDe the entity serde to set
   */
  void setSerDe(EntitySerDe entitySerDe);

  /**
   * List all the entities with the specified {@link Namespace}, and deserialize them into the
   * specified {@link Entity} object.
   *
   * <p>Note. Depends on the isolation levels provided by the underlying storage, the returned list
   * may not be consistent.
   *
   * @param namespace the namespace of the entities
   * @return the list of entities
   * @param <E> the type of the entity
   * @throws IOException if the list operation fails
   */
  <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Class<E> type)
      throws IOException;

  /**
   * Check if the entity with the specified {@link NameIdentifier} exists.
   *
   * @param ident the name identifier of the entity
   * @return true if the entity exists, false otherwise
   * @throws IOException if the check operation fails
   */
  boolean exists(NameIdentifier ident) throws IOException;

  /**
   * Store the entity into the underlying storage. If the entity already exists, it will overwrite
   * the existing entity.
   *
   * @param ident the unique identifier of the entity
   * @param e the entity to store
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   */
  default <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e)
      throws IOException {
    put(ident, e, false);
  }

  /**
   * Store the entity into the underlying storage. According to the {@code overwritten} flag, it
   * will overwrite the existing entity or throw an {@link EntityAlreadyExistsException}.
   *
   * <p>Note. The implementation should be transactional, and should be able to handle concurrent
   * store of entities.
   *
   * @param ident the unique identifier of the entity
   * @param e the entity to store
   * @param overwritten whether to overwrite the existing entity
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   * @throws EntityAlreadyExistsException if the entity already exists and the overwritten flag is
   *     set to false
   */
  <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Get the entity from the underlying storage.
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
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Class<E> e)
      throws NoSuchEntityException, IOException;

  /**
   * Delete the entity from the underlying storage by the specified {@link NameIdentifier}.
   *
   * @param ident the name identifier of the entity
   * @return true if the entity is deleted, false otherwise
   * @throws IOException if the delete operation fails
   */
  boolean delete(NameIdentifier ident) throws IOException;

  /**
   * Execute the specified {@link Executable} in a transaction.
   *
   * @param executable the executable to run
   * @param <R> the type of the return value
   * @return the return value of the executable
   * @throws IOException if the execution fails
   */
  <R> R executeInTransaction(Executable<R> executable) throws IOException;
}
