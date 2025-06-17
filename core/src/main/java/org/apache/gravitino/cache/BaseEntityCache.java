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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;

/**
 * An abstract class that provides a basic implementation for the {@link EntityCache} interface.
 * This class is abstract and cannot be instantiated directly, it is designed to be a base class for
 * other entity cache implementations.
 */
public abstract class BaseEntityCache implements EntityCache {
  protected final Config cacheConfig;

  /**
   * Constructs a new {@link BaseEntityCache} instance.
   *
   * @param config The cache configuration
   */
  public BaseEntityCache(Config config) {
    Preconditions.checkArgument(config != null, "Config must not be null");

    this.cacheConfig = config;
  }

  /**
   * Returns the {@link NameIdentifier} of the entity based on its type.
   *
   * @param entity The {@link Entity} instance.
   * @return The {@link NameIdentifier} of the entity
   */
  public static NameIdentifier getIdentFromEntity(Entity entity) {
    validateEntityHasIdentifier(entity);
    HasIdentifier hasIdentifier = (HasIdentifier) entity;

    return hasIdentifier.nameIdentifier();
  }

  /**
   * Checks if the entity is of type {@link HasIdentifier}.
   *
   * @param entity The {@link Entity} instance to check.
   */
  protected static void validateEntityHasIdentifier(Entity entity) {
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Unsupported EntityType: " + entity.type());
  }

  /**
   * Converts a list of entities to a new list with the target entity type.
   *
   * @param entities Thr original list of entities.
   * @return A list of converted entities.
   * @param <E> The type of the entities in the new list.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> List<E> convertEntities(List<Entity> entities) {
    entities.forEach(BaseEntityCache::validateEntityHasIdentifier);

    return (List<E>) (List<? extends Entity>) entities;
  }

  /**
   * Converts an entity to a new one.
   *
   * @param entity The original entity.
   * @return A new entity.
   * @param <E> The type of the new entity.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> E convertEntity(Entity entity) {
    validateEntityHasIdentifier(entity);

    return (E) entity;
  }

  /**
   * Removes an expired entity from the data cache.
   *
   * @param key The expired entity key to remove.
   */
  protected abstract void invalidateExpiredItem(EntityCacheKey key);
}
