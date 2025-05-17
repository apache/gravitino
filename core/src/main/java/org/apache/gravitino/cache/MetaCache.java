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
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;

/**
 * {@code MetaCache} is a cache interface in Gravitino designed to accelerate metadata access for
 * entities. It maintains a bidirectional mapping between an {@link Entity} and its {@code id}, as
 * well as its {@link NameIdentifier}. The cache also supports cascading removal of entries,
 * ensuring related sub-entities are cleared together.
 */
public interface MetaCache {
  /**
   * Retrieves an entity based on its id and type. If the entity is not present in the cache, it
   * will be fetched from the underlying EntityStore.
   *
   * @param id The unique id of the entity
   * @param type The type of the entity
   * @return The cached or newly loaded Entity instance
   * @param <E> The class of the entity
   */
  <E extends Entity & HasIdentifier> E getOrLoadMetadataById(Long id, Entity.EntityType type);

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
  <E extends Entity & HasIdentifier> E getOrLoadMetadataByName(
      NameIdentifier ident, Entity.EntityType type) throws IOException;

  /**
   * Removes an entity (and any sub-entities) from the cache by its id and type.
   *
   * @param id the unique id of the entity to remove
   * @param type the type of the entity
   * @return {@code true} if the entity was removed from the cache; {@code false} otherwise
   */
  boolean removeById(Long id, Entity.EntityType type);

  /**
   * Removes an entity (and any sub-entities) from the cache by its name identifier and type.
   *
   * @param ident the name identifier of the entity to remove
   * @param type the type of the entity
   * @return {@code true} if the entity was removed from the cache; {@code false} otherwise
   */
  boolean removeByName(NameIdentifier ident, Entity.EntityType type);

  /**
   * Checks whether an entity with the given id and type is present in the cache.
   *
   * @param id the unique id of the entity
   * @param type the type of the entity
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean containsById(Long id, Entity.EntityType type);

  /**
   * Checks whether an entity with the given name identifier and type is present in the cache.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean containsByName(NameIdentifier ident, Entity.EntityType type);

  /**
   * Returns the current number of entries stored in the data cache.
   *
   * @return the estimated size of the data cache
   */
  long sizeOfCacheData();

  /**
   * Returns the current number of entries stored in the index structures.
   *
   * @return the size of the index (number of indexed keys)
   */
  long sizeOfCacheIndex();

  /**
   * Clears all entries from the cache, including data and index structures, resetting it to an
   * empty state.
   */
  void clear();

  /**
   * Puts an entity into the cache.
   *
   * @param entity The entity to cache
   * @param <E> The class of the entity
   */
  <E extends Entity & HasIdentifier> void put(E entity);
}
