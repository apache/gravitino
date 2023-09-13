/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.exceptions.AlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchEntityException;
import com.datastrato.graviton.utils.Executable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

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
   * @param type the type of the entity
   * @throws IOException if the list operation fails
   */
  <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, EntityType entityType) throws IOException;

  /**
   * Check if the entity with the specified {@link NameIdentifier} exists.
   *
   * @param ident the name identifier of the entity
   * @return true if the entity exists, false otherwise
   * @throws IOException if the check operation fails
   */
  boolean exists(NameIdentifier ident, EntityType entityType) throws IOException;

  /**
   * Store the entity into the underlying storage. If the entity already exists, it will overwrite
   * the existing entity.
   *
   * @param e the entity to store
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   */
  default <E extends Entity & HasIdentifier> void put(E e) throws IOException {
    put(e, false);
  }

  /**
   * Store the entity into the underlying storage. According to the {@code overwritten} flag, it
   * will overwrite the existing entity or throw an {@link EntityAlreadyExistsException}.
   *
   * <p>Note. The implementation should be transactional, and should be able to handle concurrent
   * store of entities.
   *
   * @param e the entity to store
   * @param overwritten whether to overwrite the existing entity
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   * @throws EntityAlreadyExistsException if the entity already exists and the overwritten flag is
   *     set to false
   */
  <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Update the entity into the underlying storage.
   *
   * <p>Note: the {@link NameIdentifier} of the updated entity may be changed. Based on the design
   * of the storage key, the implementation may have two implementations.
   *
   * <p>1) if the storage key is name relevant, it needs to delete the old entity and store the new
   * entity with the new key. 2) or if the storage key is name irrelevant, it can just store the new
   * entity with the current key.
   *
   * <p>Note: the whole update operation should be in one transaction.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @param updater the updater function to update the entity
   * @param <E> the type of the entity
   * @return E the updated entity
   * @throws IOException if the store operation fails
   * @throws NoSuchEntityException if the entity does not exist
   * @throws AlreadyExistsException if the updated entity already exitsed.
   */
  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException;

  /**
   * Get the entity from the underlying storage.
   *
   * <p>Note. The implementation should be thread-safe, and should be able to handle concurrent
   * retrieve of entities.
   *
   * @param ident the unique identifier of the entity
   * @return the entity retrieved from the underlying storage
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the retrieve operation fails
   */
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException;

  /**
   * Delete the entity from the underlying storage by the specified {@link NameIdentifier}.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity to be deleted
   * @return true if the entity exists and is deleted successfully, false otherwise
   * @throws IOException if the delete operation fails
   */
  default boolean delete(NameIdentifier ident, EntityType entityType) throws IOException {
    return delete(ident, entityType, false);
  }

  /**
   * Delete the entity from the underlying storage by the specified {@link NameIdentifier}.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity to be deleted
   * @param cascade support cacade detele or not
   * @return true if the entity exists and is deleted successfully, false otherwise
   * @throws IOException if the delete operation fails
   */
  boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade) throws IOException;

  /**
   * Execute the specified {@link Executable} in a transaction.
   *
   * @param executable the executable to run
   * @param <R> the type of the return value
   * @return the return value of the executable
   * @throws IOException if the execution fails
   */
  <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException;
}
