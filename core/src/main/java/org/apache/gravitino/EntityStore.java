/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.tag.SupportsTagOperations;
import org.apache.gravitino.utils.Executable;

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
   * List all the entities with the specified {@link org.apache.gravitino.Namespace}, and
   * deserialize them into the specified {@link Entity} object.
   *
   * <p>Note. Depends on the isolation levels provided by the underlying storage, the returned list
   * may not be consistent.
   *
   * @param <E> class of the entity
   * @param namespace the namespace of the entities
   * @param type the detailed type of the entity
   * @param entityType the general type of the entity
   * @return the list of entities
   * @throws IOException if the list operation fails
   */
  default <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, EntityType entityType) throws IOException {
    return list(namespace, type, entityType, true /* allFields */);
  }

  /**
   * List all the entities with the specified {@link org.apache.gravitino.Namespace}, and
   * deserialize them into the specified {@link Entity} object.
   *
   * <p>Note. Depends on the isolation levels provided by the underlying storage, the returned list
   * may not be consistent.
   *
   * @param <E> class of the entity
   * @param namespace the namespace of the entities
   * @param type the detailed type of the entity
   * @param entityType the general type of the entity
   * @param allFields Some fields may have a relatively high acquisition cost, EntityStore provides
   *     an optional setting to avoid fetching these high-cost fields to improve the performance. If
   *     true, the method will fetch all the fields, Otherwise, the method will fetch all the fields
   *     except for high-cost fields.
   * @return the list of entities
   * @throws IOException if the list operation fails
   */
  default <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> type, EntityType entityType, boolean allFields)
      throws IOException {
    throw new UnsupportedOperationException("Don't support to skip fields");
  }

  /**
   * Check if the entity with the specified {@link org.apache.gravitino.NameIdentifier} exists.
   *
   * @param ident the name identifier of the entity
   * @param entityType the general type of the entity,
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
   * <p>Note: the {@link org.apache.gravitino.NameIdentifier} of the updated entity may be changed.
   * Based on the design of the storage key, the implementation may have two implementations.
   *
   * <p>1) if the storage key is name relevant, it needs to delete the old entity and store the new
   * entity with the new key. 2) or if the storage key is name irrelevant, it can just store the new
   * entity with the current key.
   *
   * <p>Note: the whole update operation should be in one transaction.
   *
   * @param ident the name identifier of the entity
   * @param type the detailed type of the entity
   * @param updater the updater function to update the entity
   * @param <E> the class of the entity
   * @param entityType the general type of the entity
   * @return E the updated entity
   * @throws IOException if the store operation fails
   * @throws NoSuchEntityException if the entity does not exist
   * @throws EntityAlreadyExistsException if the updated entity already existed.
   */
  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException;

  /**
   * Get the entity from the underlying storage.
   *
   * <p>Note. The implementation should be thread-safe, and should be able to handle concurrent
   * retrieve of entities.
   *
   * @param ident the unique identifier of the entity
   * @param entityType the general type of the entity
   * @param e the entity class instance
   * @param <E> the class of entity
   * @return the entity retrieved from the underlying storage
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the retrieve operation fails
   */
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException;

  /**
   * Delete the entity from the underlying storage by the specified {@link
   * org.apache.gravitino.NameIdentifier}.
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
   * Delete the entity from the underlying storage by the specified {@link
   * org.apache.gravitino.NameIdentifier}.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity to be deleted
   * @param cascade support cascade delete or not
   * @return true if the entity exists and is deleted successfully, false otherwise
   * @throws IOException if the delete operation fails
   */
  boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade) throws IOException;

  /**
   * Batch delete entities from the underlying storage by the specified list of {@link
   * org.apache.gravitino.NameIdentifier} and {@link EntityType}.
   *
   * @param entitiesToDelete the list of pairs of name identifiers and entity types to be deleted
   * @param cascade if true, cascade delete the entities, otherwise just delete the entities
   * @return the number of entities deleted
   * @throws IOException if the batch delete operation fails
   */
  int batchDelete(List<Pair<NameIdentifier, EntityType>> entitiesToDelete, boolean cascade)
      throws IOException;

  /**
   * Batch put entities into the underlying storage.
   *
   * @param entities the list of entities to be stored
   * @param overwritten if true, overwrite the existing entities, otherwise throw an
   * @param <E> the type of the entities
   * @throws IOException if the batch put operation fails
   * @throws EntityAlreadyExistsException if the entity already exists and the overwritten flag is
   *     false
   */
  <E extends Entity & HasIdentifier> void batchPut(List<E> entities, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Execute the specified {@link Executable} in a transaction.
   *
   * @param executable the executable to run
   * @param <R> the type of the return value
   * @param <E> the type of the exception
   * @return the return value of the executable
   * @throws IOException if the execution fails
   * @throws E if the execution fails
   */
  <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException;

  /**
   * Get the extra tag operations that are supported by the entity store.
   *
   * @return the tag operations object that are supported by the entity store
   * @throws UnsupportedOperationException if the extra operations are not supported
   */
  default SupportsTagOperations tagOperations() {
    throw new UnsupportedOperationException("tag operations are not supported");
  }

  /**
   * Get the extra relation operations that are supported by the entity store.
   *
   * @return the relation operations that are supported by the entity store
   * @throws UnsupportedOperationException if the extra operations are not supported
   */
  default SupportsRelationOperations relationOperations() {
    throw new UnsupportedOperationException("relation operations are not supported");
  }
}
