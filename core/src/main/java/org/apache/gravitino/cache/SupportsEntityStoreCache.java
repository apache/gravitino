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

import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;

/**
 * {@code StoreEntityCache} defines caching operations for direct entity access. It supports
 * loading, storing, and invalidating individual metadata entities.
 */
public interface SupportsEntityStoreCache {
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
   * Invalidates the cache entry for the given entity.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @return true if the cache entry was removed
   */
  boolean invalidate(NameIdentifier ident, Entity.EntityType type);

  /**
   * Checks whether an entity with the given name identifier and type is present in the cache.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean contains(NameIdentifier ident, Entity.EntityType type);

  /**
   * Puts an entity into the cache.
   *
   * @param entity The entity to cache
   * @param <E> The class of the entity
   */
  <E extends Entity & HasIdentifier> void put(E entity);

  /**
   * Invalidates related cache entries when inserting the given entity, if necessary.
   *
   * <p>For example, inserting a {@code ModelVersion} may require invalidating the corresponding
   * {@code Model} entry in the cache to maintain consistency.
   *
   * @param entity The entity being inserted
   * @param <E> The type of the entity
   */
  <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity);
}
