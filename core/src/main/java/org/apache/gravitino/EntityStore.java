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
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.utils.Executable;

/**
 * Stores Gravitino metadata entities.
 *
 * <p>Several Gravitino servers may share one store, so every guarantee below also holds between
 * servers, not only between threads of one server.
 *
 * <h2>Writes</h2>
 *
 * <ul>
 *   <li>Each write ({@link #put}, {@link #update}, {@link #delete}, {@link #deleteAndGet}, {@link
 *       #batchPut} and {@link #batchDelete}) is atomic: it takes effect completely, including the
 *       data the entity owns such as columns, versions and relations, or not at all.
 *   <li>Concurrent writes to the same entity are resolved optimistically. The write that loses
 *       fails with {@link OptimisticLockException} and changes nothing. A write that creates an
 *       entity does not lose to a concurrent write this way; a concurrent create of the same name
 *       fails with {@link EntityAlreadyExistsException} instead.
 *   <li>The store never retries a failed write. The caller decides whether to read the current
 *       state and try again, or to report the conflict.
 * </ul>
 *
 * <h2>Outcomes</h2>
 *
 * <table border="1">
 *   <caption>Result of each operation in each situation</caption>
 *   <tr><th>Situation</th><th>Result</th></tr>
 *   <tr><td>The entity does not exist</td>
 *       <td>{@link #get} and {@link #update} throw {@link NoSuchEntityException}; {@link #delete}
 *       returns {@code false}; {@link #deleteAndGet} returns an empty result; {@link #exists}
 *       returns {@code false}; {@link #batchGet} leaves it out of the result.</td></tr>
 *   <tr><td>The parent of a new entity does not exist</td>
 *       <td>{@link #put} throws {@link NoSuchEntityException}.</td></tr>
 *   <tr><td>The name is already taken</td>
 *       <td>{@link #put} without overwrite and a renaming {@link #update} throw {@link
 *       EntityAlreadyExistsException}.</td></tr>
 *   <tr><td>A concurrent write to the entity won</td>
 *       <td>{@link #update}, {@link #delete} and {@link #deleteAndGet} throw {@link
 *       OptimisticLockException}. If that write deleted or renamed the entity, the result is the
 *       one for an entity that does not exist instead.</td></tr>
 *   <tr><td>A non-cascade delete finds children</td>
 *       <td>{@link #delete} throws {@link NonEmptyEntityException}.</td></tr>
 *   <tr><td>The store cannot hold the entity type</td>
 *       <td>The operation throws {@link UnsupportedEntityTypeException}; {@link #batchPut} and
 *       {@link #batchDelete} throw {@link IllegalArgumentException}.</td></tr>
 *   <tr><td>The storage itself fails</td>
 *       <td>The operation throws {@link IOException} or a runtime exception; callers must handle
 *       both.</td></tr>
 * </table>
 *
 * <h2>Reads</h2>
 *
 * <p>{@link #get}, {@link #batchGet} and {@link #exists} may be answered from a per-server cache.
 * Changes made on this server are visible to its next read, but changes made on another server
 * reach the cache only after a delay. Such a read can therefore return an entity that another
 * server has already changed or deleted, and {@link #exists} can return {@code true} for it. A
 * {@code false} from {@link #exists} and every {@link #list} read the storage. Writes always act on
 * the stored entity, never on a cached copy.
 */
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
   * deserialize them into the specified {@link Entity} object. This is the same as {@link
   * #list(Namespace, Class, EntityType, boolean)} with {@code allFields} set to {@code false}.
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
    return list(namespace, type, entityType, false /* allFields */);
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
   * <p>A {@code true} result may come from the cache and be stale; a {@code false} result reflects
   * the storage. See the class documentation.
   *
   * @param ident the name identifier of the entity
   * @param entityType the general type of the entity,
   * @return true if the entity exists, false otherwise
   * @throws IOException if the check operation fails
   */
  boolean exists(NameIdentifier ident, EntityType entityType) throws IOException;

  /**
   * Store a new entity into the underlying storage. This is the same as {@link #put(Entity,
   * boolean)} without overwrite: it fails if an entity with the same name already exists.
   *
   * @param e the entity to store
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   * @throws EntityAlreadyExistsException if an entity with the same name already exists
   * @throws NoSuchEntityException if the parent of the entity does not exist
   */
  default <E extends Entity & HasIdentifier> void put(E e) throws IOException {
    put(e, false);
  }

  /**
   * Store the entity into the underlying storage. According to the {@code overwritten} flag, it
   * will overwrite the existing entity or throw an {@link EntityAlreadyExistsException}.
   *
   * <p>Without overwrite, the insert is atomic with respect to other inserts: of several concurrent
   * inserts of the same name, exactly one succeeds. With overwrite, whether the stored entity keeps
   * the id of the entity it replaces depends on the implementation; callers must not rely on
   * either. A model version is always inserted as a new version, and the flag is ignored.
   *
   * @param e the entity to store
   * @param overwritten whether to overwrite the existing entity
   * @param <E> the type of the entity
   * @throws IOException if the store operation fails
   * @throws EntityAlreadyExistsException if the entity already exists and the overwritten flag is
   *     set to false
   * @throws NoSuchEntityException if the parent of the entity does not exist
   */
  <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Update the entity into the underlying storage.
   *
   * <p>The store reads the entity from the storage, not from the cache, and calls {@code updater}
   * exactly once with it. The updater returns the new state of the entity; throwing from it aborts
   * the update with nothing written. The updater runs before the write is known to succeed, so it
   * must not have effects outside the returned entity. If another write to the entity commits
   * between the read and this write, the update fails with {@link OptimisticLockException}, or with
   * {@link NoSuchEntityException} if that write deleted or renamed the entity.
   *
   * <p>The updater may change the name, which renames the entity; the new name must be free. It
   * must not change the id, and an implementation rejects such an update with {@link
   * IllegalArgumentException}. After a rename, neither the old nor the new name is served from a
   * stale cache entry.
   *
   * @param ident the name identifier of the entity
   * @param type the detailed type of the entity
   * @param updater the updater function to update the entity
   * @param <E> the class of the entity
   * @param entityType the general type of the entity
   * @return E the updated entity
   * @throws IOException if the store operation fails
   * @throws NoSuchEntityException if the entity does not exist, or a concurrent write deleted or
   *     renamed it
   * @throws EntityAlreadyExistsException if the entity is renamed to a name that is already taken
   * @throws OptimisticLockException if a concurrent write to the entity committed first
   */
  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException;

  /**
   * Get the entity from the underlying storage.
   *
   * <p>The result may come from the cache and not yet reflect a change made on another server. See
   * the class documentation.
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
   * Batch get the entities from the underlying storage. All identifiers must be in the same
   * namespace.
   *
   * <p>An identifier with no entity is left out of the result instead of failing the call, so the
   * result may be shorter than {@code idents}; a failure to read the storage is thrown. The order
   * of the result is unspecified. Like {@link #get}, entities may come from the cache.
   *
   * @param idents the unique identifiers of the entities
   * @param entityType the general type of the entities
   * @param clazz the entity class instance
   * @param <E> the class of entity
   * @return the entities that exist, in unspecified order
   * @throws UnsupportedEntityTypeException if the store cannot batch get this entity type
   * @throws IllegalArgumentException if the identifiers are not all in the same namespace
   */
  <E extends Entity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, EntityType entityType, Class<E> clazz);

  /**
   * Batch get the entities from the underlying storage. All identifiers must be in the same
   * namespace.
   *
   * <p>An identifier with no entity is left out of the result instead of failing the call, so the
   * result may be shorter than {@code idents}; a failure to read the storage is thrown. The order
   * of the result is unspecified. Like {@link #get}, entities may come from the cache.
   *
   * @param idents the unique identifiers of the entities
   * @param entityType the general type of the entities
   * @param clazz the entity class instance
   * @param <E> the class of entity
   * @return the entities that exist, in unspecified order
   * @throws UnsupportedEntityTypeException if the store cannot batch get this entity type
   * @throws IllegalArgumentException if the identifiers are not all in the same namespace
   */
  default <E extends Entity & HasIdentifier> E[] batchGet(
      NameIdentifier[] idents, EntityType entityType, Class<E> clazz) {
    return batchGet(Arrays.asList(idents), entityType, clazz)
        .toArray(size -> (E[]) Array.newInstance(clazz, size));
  }

  /**
   * Delete the entity from the underlying storage by the specified {@link
   * org.apache.gravitino.NameIdentifier}. This is the same as {@link #delete(NameIdentifier,
   * EntityType, boolean)} without cascade.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity to be deleted
   * @return true if the entity exists and is deleted successfully, false if it does not exist
   * @throws IOException if the delete operation fails
   */
  default boolean delete(NameIdentifier ident, EntityType entityType) throws IOException {
    return delete(ident, entityType, false);
  }

  /**
   * Delete the entity from the underlying storage by the specified {@link
   * org.apache.gravitino.NameIdentifier}.
   *
   * <p>A missing entity is reported by returning {@code false}, never by throwing {@link
   * NoSuchEntityException}. Deleting an entity also deletes the data it owns, such as columns,
   * versions, and its tag, policy, owner and privilege relations.
   *
   * <p>{@code cascade} applies to entities that contain other entities: a metalake (catalogs), a
   * catalog (schemas) and a schema (tables, views, filesets, topics, functions, models, semantic
   * models and nested schemas). Without cascade, deleting such an entity while it still has
   * children fails with {@link NonEmptyEntityException}; with cascade, the children are deleted
   * too. For every other entity type the flag has no effect.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity to be deleted
   * @param cascade whether to delete the children of a metalake, catalog or schema
   * @return true if the entity exists and is deleted successfully, false if it does not exist
   * @throws IOException if the delete operation fails
   * @throws NonEmptyEntityException if {@code cascade} is false and the entity has children
   * @throws OptimisticLockException if a concurrent write to the entity committed first
   */
  boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade) throws IOException;

  /**
   * The only post-delete action an implementation that cannot run it before commit accepts.
   *
   * <p>Compared by reference, so a caller that supplies its own action reaches an implementation
   * that honors the contract or gets told that this one cannot.
   */
  Consumer<? extends Entity> NO_POST_DELETE_ACTION = ignored -> {};

  /**
   * Returns the shared no-op post-delete action.
   *
   * @param <E> the entity type
   * @return an action that does nothing
   */
  @SuppressWarnings("unchecked")
  static <E extends Entity & HasIdentifier> Consumer<E> noPostDeleteAction() {
    return (Consumer<E>) NO_POST_DELETE_ACTION;
  }

  /**
   * Deletes an entity and returns the snapshot chosen by the delete operation.
   *
   * <p>The default implementation reads the entity and then deletes it, so the returned snapshot
   * may differ from the entity that was deleted if a concurrent write lands in between. Stores that
   * can read and delete in one atomic step should override this method so the returned snapshot is
   * exactly the one that was deleted.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity
   * @param clazz the concrete entity class
   * @param <E> the entity type
   * @return the deleted entity, or empty when it did not exist
   * @throws IOException if the delete operation fails
   */
  default <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident, EntityType entityType, Class<E> clazz) throws IOException {
    return deleteAndGet(ident, entityType, clazz, noPostDeleteAction());
  }

  /**
   * Deletes an entity, runs an action against the deleted snapshot, and returns that snapshot.
   *
   * <p>A transactional store should run the action after its delete has won but before committing.
   * This lets callers couple non-database cleanup to the metadata transaction: an action failure
   * can still roll the metadata delete back.
   *
   * @param ident the name identifier of the entity
   * @param entityType the type of the entity
   * @param clazz the concrete entity class
   * @param postDeleteAction the action to run after deletion but before commit when supported
   * @param <E> the entity type
   * @return the deleted entity, or empty when it did not exist
   * @throws IOException if the delete operation fails
   */
  default <E extends Entity & HasIdentifier> Optional<E> deleteAndGet(
      NameIdentifier ident, EntityType entityType, Class<E> clazz, Consumer<E> postDeleteAction)
      throws IOException {
    if (postDeleteAction != NO_POST_DELETE_ACTION) {
      // This implementation can only run the action once the delete is committed, which is the
      // opposite of what the contract promises. Refusing is better than silently leaving the
      // caller with a committed delete and a failed cleanup.
      throw new UnsupportedOperationException(
          "This store cannot run a post-delete action while the delete can still be rolled back");
    }

    try {
      E entity = get(ident, entityType, clazz);
      if (!delete(ident, entityType)) {
        return Optional.empty();
      }
      postDeleteAction.accept(entity);
      return Optional.of(entity);
    } catch (NoSuchEntityException e) {
      return Optional.empty();
    }
  }

  /**
   * Batch delete entities from the underlying storage by the specified list of {@link
   * org.apache.gravitino.NameIdentifier} and {@link EntityType}.
   *
   * <p>Only some entity types support batch deletion, and all entities must have the same type.
   *
   * @param entitiesToDelete the list of pairs of name identifiers and entity types to be deleted
   * @param cascade if true, cascade delete the entities, otherwise just delete the entities
   * @return the number of entities deleted
   * @throws IOException if the batch delete operation fails
   * @throws IllegalArgumentException if the entity type or the combination of arguments is not
   *     supported
   */
  int batchDelete(List<Pair<NameIdentifier, EntityType>> entitiesToDelete, boolean cascade)
      throws IOException;

  /**
   * Batch put entities into the underlying storage.
   *
   * <p>Only some entity types support batch insertion, and all entities must have the same type.
   *
   * @param entities the list of entities to be stored
   * @param overwritten if true, overwrite the existing entities, otherwise throw an {@link
   *     EntityAlreadyExistsException}
   * @param <E> the type of the entities
   * @throws IOException if the batch put operation fails
   * @throws EntityAlreadyExistsException if the entity already exists and the overwritten flag is
   *     false
   * @throws IllegalArgumentException if the entity type or the combination of arguments is not
   *     supported
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
   * @throws UnsupportedOperationException unless an implementation overrides this method
   * @deprecated The store does not support transactions composed by the caller: each write method
   *     is atomic on its own (see the class documentation), and the relational store never
   *     implemented this method. Atomicity across several entities is tracked in <a
   *     href="https://github.com/apache/gravitino/issues/13632">#13632</a>. This method will be
   *     removed in a future release.
   */
  @Deprecated
  default <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    throw new UnsupportedOperationException(
        "The entity store does not support transactions composed by the caller");
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
