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
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;

/**
 * An abstract class that provides a basic implementation for the {@link EntityCache} interface.
 * This class is abstract and cannot be instantiated directly, it is designed to be a base class for
 * other entity cache implementations.
 *
 * <p>This class enforces the cacheable entity type allowlist documented on {@link
 * SupportsEntityStoreCache#put(Entity)}: {@link #put(Entity)} is final and drops entities that have
 * not been explicitly approved before delegating to {@link #doPut(Entity)}, so no subclass can
 * accidentally cache them.
 */
public abstract class BaseEntityCache implements EntityCache {

  /**
   * Entity types that have been explicitly approved for caching, see {@link
   * SupportsEntityStoreCache#put(Entity)} for the contract. New entity types are excluded by
   * default until their invalidation behavior has been validated.
   *
   * <p>{@code USER}, {@code GROUP} and {@code ROLE} are materialized with relation-derived data
   * joined in at load time: a role carries its securable objects, and a user/group carries its role
   * names. A mutation on the entity itself invalidates its own key through the write path, but this
   * embedded data also goes stale through a mutation on a different entity. For example, deleting
   * or renaming a securable object changes a role's materialized form, and deleting or renaming a
   * role changes a user's/group's role names. Such a mutation touches neither this entity's own key
   * nor any hierarchy ancestor of it, so neither the write-path invalidation nor a prefix cascade
   * over the entity hierarchy would evict it. Caching them would therefore serve stale
   * authorization data.
   */
  private static final Set<Entity.EntityType> CACHEABLE_TYPES =
      Sets.immutableEnumSet(
          Entity.EntityType.METALAKE,
          Entity.EntityType.CATALOG,
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.VIEW,
          Entity.EntityType.COLUMN,
          Entity.EntityType.FILESET,
          Entity.EntityType.TOPIC,
          Entity.EntityType.TAG,
          Entity.EntityType.MODEL,
          Entity.EntityType.MODEL_VERSION,
          Entity.EntityType.POLICY,
          Entity.EntityType.TABLE_STATISTIC,
          Entity.EntityType.JOB_TEMPLATE,
          Entity.EntityType.JOB,
          Entity.EntityType.AUDIT,
          Entity.EntityType.FUNCTION);

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
   * Returns whether entities of the given type may be cached.
   *
   * @param type The entity type to check.
   * @return {@code true} if entities of this type may be cached, {@code false} otherwise.
   */
  public static boolean isCacheable(Entity.EntityType type) {
    Preconditions.checkArgument(type != null, "Entity type cannot be null");

    return CACHEABLE_TYPES.contains(type);
  }

  /** {@inheritDoc} */
  @Override
  public final <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    // Called before the cacheability check and before any subclass takes this entity's lock: it
    // may take another key's lock (e.g. the model key when inserting a model version), and nesting
    // two entity locks could deadlock. A non-cacheable entity can still invalidate a cacheable one,
    // so this runs for every entity.
    invalidateOnKeyChange(entity);

    if (!isCacheable(entity.type())) {
      return;
    }

    doPut(entity);
  }

  /**
   * Caches the given entity. Called by {@link #put(Entity)} once the entity is known to be
   * non-null, cacheable, and any related cache entries have been invalidated.
   *
   * @param entity The entity to cache.
   * @param <E> The class of the entity.
   */
  protected abstract <E extends Entity & HasIdentifier> void doPut(E entity);

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
    Preconditions.checkArgument(entity != null, "Entity cannot be null");
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
