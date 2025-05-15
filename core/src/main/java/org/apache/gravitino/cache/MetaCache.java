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
import org.apache.gravitino.NameIdentifier;

/** MetaCache defines a generic caching interface for metadata entities in Gravitino. */
public interface MetaCache {
  /**
   * Retrieves an entity based on its id and type. If the entity is not present in the cache, it
   * will be fetched from the underlying EntityStore.
   *
   * @param id the unique id of the entity
   * @param type the type of the entity
   * @return the cached or newly loaded Entity instance
   */
  Entity getOrLoadMetadataById(Long id, Entity.EntityType type);

  /**
   * Retrieves an entity by its name identifier and type. If the entity is not present in the cache,
   * it will be loaded from the backing EntityStore.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @return the cached or newly loaded Entity instance
   * @throws IOException if loading the entity from the store fails
   */
  Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) throws IOException;

  /**
   * Removes an entity (and any sub-entities) from the cache by its id and type.
   *
   * @param id the unique id of the entity to remove
   * @param type the type of the entity
   */
  void removeById(Long id, Entity.EntityType type);

  /**
   * Removes an entity (and any sub-entities) from the cache by its name identifier.
   *
   * @param ident the name identifier of the entity to remove
   */
  void removeByName(NameIdentifier ident);

  /**
   * Checks whether an entity with the given id and type is present in the cache.
   *
   * @param id the unique id of the entity
   * @param type the type of the entity
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean containsById(Long id, Entity.EntityType type);

  /**
   * Checks whether an entity with the given name identifier is present in the cache.
   *
   * @param ident the name identifier of the entity
   * @return true if the entity is cached; false otherwise
   */
  boolean containsByName(NameIdentifier ident);

  /**
   * Clears all entries from the cache, including data and index structures, resetting it to an
   * empty state.
   */
  void clear();

  /**
   * Returns the current number of entries stored in the data cache.
   *
   * @return the estimated size of the data cache
   */
  long sizeOfCacheData();

  /**
   * Put an entity into the cache.
   *
   * @param entity the entity to cache.
   */
  void put(Entity entity);

  /**
   * Returns the current number of entries stored in the index structures.
   *
   * @return the size of the index (number of indexed keys)
   */
  long sizeOfCacheIndex();
}
