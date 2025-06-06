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

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;

/**
 * {@code RelationEntityCache} defines relation-specific caching behavior for entity-to-entity
 * relationships.
 */
public interface SupportsRelationEntityCache {
  /**
   * Retrieves a list of related entities from the cache, if present.
   *
   * @param relType the relation type
   * @param nameIdentifier the name identifier of the entity to find related entities for
   * @param identType the identifier type of the related entities to find
   * @return a list of related entities, or an empty list if none are found
   * @param <E> The class of the related entities
   */
  <E extends Entity & HasIdentifier> Optional<List<E>> getIfPresent(
      SupportsRelationOperations.Type relType,
      NameIdentifier nameIdentifier,
      Entity.EntityType identType);

  /**
   * Invalidates the cached relation for the given entity and relation type.
   *
   * @param ident the name identifier
   * @param type the entity type
   * @param relType the relation type
   * @return true if the cache entry was removed
   */
  boolean invalidate(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType);

  /**
   * Checks whether an entity with the given name identifier, type, and relation type is present in
   * the cache.
   *
   * @param ident the name identifier of the entity
   * @param type the type of the entity
   * @param relType the relation type
   * @return {@code true} if the entity is cached; {@code false} otherwise
   */
  boolean contains(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relType);

  /**
   * Puts a list of related entities into the cache.
   *
   * @param ident The name identifier of the entity to cache the related entities for
   * @param type The type of the entity to cache the related entities for
   * @param relType The relation type to cache the related entities for
   * @param entities The list of related entities to cache
   * @param <E> The class of the related entities
   */
  <E extends Entity & HasIdentifier> void put(
      NameIdentifier ident,
      Entity.EntityType type,
      SupportsRelationOperations.Type relType,
      List<E> entities);
}
