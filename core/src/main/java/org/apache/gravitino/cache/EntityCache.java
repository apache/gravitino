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

package org.apache.gravitino.cache;

import java.io.IOException;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;

/**
 * {@code EntityCache} is a cache interface in Gravitino designed to accelerate metadata access for
 * entities. It maintains a bidirectional mapping between an {@link Entity} and its {@code
 * NameIdentifier}. The cache also supports cascading removal of entries, ensuring related
 * sub-entities are cleared together.
 */
public interface EntityCache {
  /**
   * Retrieves an entity by its name identifier and type. If the entity is not present in the cache,
   * it will be loaded from the backing EntityStore.
   *
   * @param ident The name identifier of the entity
   * @param type The type of the entity
   * @return The cached or newly loaded Entity instance
   * @param <E> The class of the entity
   * @throws IOException if the operation fails
   */
  <E extends Entity & HasIdentifier> E getOrLoad(NameIdentifier ident, Entity.EntityType type)
      throws IOException;

  /**
   * Retrieves an entity from the cache if it exists. Will not attempt to load from the store if
   * missing.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @param <E> the entity class
   * @return an optional entity if cached
   */
  <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type);

  /**
   * Invalidates the cache entry for the given entity. Does not affect the underlying storage.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @return true if the cache entry was removed
   */
  default boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    return remove(ident, type, false);
  }

  /**
   * Removes the entity from both cache and optionally the underlying store.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @param removeFromStore {@code true} to also delete from store
   * @return {@code true} if removed from cache or store, {@code false} if cache does nothing.
   */
  boolean remove(NameIdentifier ident, Entity.EntityType type, boolean removeFromStore);

  /**
   * Shortcut to remove from both cache and store.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @return true if removed
   */
  default boolean remove(NameIdentifier ident, Entity.EntityType type) {
    return remove(ident, type, true);
  }

  /**
   * Checks whether an entity with the given name identifier and type is present in the cache.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean contains(NameIdentifier ident, Entity.EntityType type);

  /**
   * Returns the current number of entries stored in the data cache.
   *
   * @return the estimated size of the data cache
   */
  long size();

  /**
   * Clears all entries from the cache, including data and index, resetting it to an empty state.
   */
  void clear();

  /**
   * Puts an entity into the cache.
   *
   * @param entity The entity to cache
   * @param <E> The class of the entity
   */
  <E extends Entity & HasIdentifier> void put(E entity);

  /**
   * Executes the given action within a cache context.
   *
   * @param action The action to cache
   */
  void withCacheLock(Runnable action);
}
