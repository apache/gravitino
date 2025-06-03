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
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.relational.RelationalEntityStore;

/**
 * An abstract class that provides a basic implementation for the {@link EntityCache} interface.
 * This class is abstract and cannot be instantiated directly, it is designed to be a base class for
 * other entity cache implementations.
 */
public abstract class BaseEntityCache implements EntityCache {
  private static final Map<Entity.EntityType, Class<?>> ENTITY_CLASS_MAP;
  // The entity store used by the cache, initialized through the constructor.
  protected final RelationalEntityStore entityStore;
  protected final Config cacheConfig;

  static {
    Map<Entity.EntityType, Class<?>> map = new EnumMap<>(Entity.EntityType.class);
    map.put(Entity.EntityType.METALAKE, BaseMetalake.class);
    map.put(Entity.EntityType.CATALOG, CatalogEntity.class);
    map.put(Entity.EntityType.SCHEMA, SchemaEntity.class);
    map.put(Entity.EntityType.TABLE, TableEntity.class);
    map.put(Entity.EntityType.FILESET, FilesetEntity.class);
    map.put(Entity.EntityType.MODEL, ModelEntity.class);
    map.put(Entity.EntityType.TOPIC, TopicEntity.class);
    map.put(Entity.EntityType.TAG, TagEntity.class);
    map.put(Entity.EntityType.MODEL_VERSION, ModelVersionEntity.class);
    map.put(Entity.EntityType.COLUMN, ColumnEntity.class);
    map.put(Entity.EntityType.USER, UserEntity.class);
    map.put(Entity.EntityType.GROUP, Entity.class);
    map.put(Entity.EntityType.ROLE, RoleEntity.class);
    ENTITY_CLASS_MAP = Collections.unmodifiableMap(map);
  }

  /**
   * Constructs a new {@link BaseEntityCache} instance.
   *
   * @param entityStore The entity store to be used by the cache.
   */
  public BaseEntityCache(Config config, EntityStore entityStore) {
    Preconditions.checkArgument(config != null, "Config must not be null");
    Preconditions.checkArgument(entityStore != null, "EntityStore must not be null");

    this.cacheConfig = config;
    this.entityStore = (RelationalEntityStore) entityStore;
  }

  /**
   * Returns the class of the entity based on its type.
   *
   * @param type The entity type
   * @return The class of the entity
   * @throws IllegalArgumentException if the entity type is not supported
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> Class<E> getEntityClass(Entity.EntityType type) {
    Preconditions.checkArgument(type != null, "EntityType must not be null");

    Class<?> clazz = ENTITY_CLASS_MAP.get(type);
    Preconditions.checkArgument(clazz != null, "Unsupported EntityType: " + type);

    return (Class<E>) clazz;
  }

  /**
   * Returns the {@link NameIdentifier} of the entity based on its type.
   *
   * @param entity The {@link Entity} instance.
   * @return The {@link NameIdentifier} of the entity
   */
  protected static NameIdentifier getIdentFromEntity(Entity entity) {
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
  protected static <E extends Entity & HasIdentifier> List<E> convertEntity(List<Entity> entities) {
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
  protected static <E extends Entity & HasIdentifier> E convertEntity(Entity entity) {
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
