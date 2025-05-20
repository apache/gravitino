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
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
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

/**
 * An abstract class that provides a basic implementation for the MetaCache interface. This class is
 * abstract and cannot be instantiated directly, it is designed to be a base class for other meta
 * cache implementations.
 *
 * <p>The purpose of the BaseMetaCache is to provide a unified way of accessing entity stores,
 * allowing subclasses to focus on caching logic without having to deal with entity store
 * management.
 */
public abstract class BaseEntityCache implements EntityCache {
  private static final Map<Entity.EntityType, Class<?>> ENTITY_CLASS_MAP;
  private static final String NULL_ENTITY_STORE =
      "EntityStore cannot be null if you want to operate underlying " + "database";
  // The entity store used by the cache, initialized through the constructor.
  protected final EntityStore entityStore;
  protected final CacheConfig cacheConfig;

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
   * Returns the class of the entity based on its type.
   *
   * @param type The entity type
   * @return The class of the entity
   * @throws IllegalArgumentException if the entity type is not supported
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> Class<E> getEntityClass(Entity.EntityType type) {
    Preconditions.checkNotNull(type, "EntityType must not be null");

    Class<?> aClass = ENTITY_CLASS_MAP.get(type);
    if (aClass == null) {
      throw new IllegalArgumentException("Unsupported EntityType: " + type.getShortName());
    }

    return (Class<E>) aClass;
  }

  /**
   * Returns the {@link NameIdentifier} of the metadata based on its type.
   *
   * @param metadata The entity
   * @return The {@link NameIdentifier} of the metadata
   */
  public static NameIdentifier getIdentFromMetadata(Entity metadata) {

    if (metadata instanceof HasIdentifier) {
      HasIdentifier hasIdentifier = (HasIdentifier) metadata;
      return hasIdentifier.nameIdentifier();
    }

    throw new IllegalArgumentException("Unsupported EntityType: " + metadata.type().getShortName());
  }

  /**
   * Constructs a new {@link BaseEntityCache} instance. If the provided entityStore is null, it will
   * use the entity store configured in the Gravitino environment.
   *
   * @param entityStore The entity store to be used by the cache, can be null.
   */
  public BaseEntityCache(CacheConfig cacheConfig, EntityStore entityStore) {
    this.cacheConfig = cacheConfig;
    this.entityStore = entityStore;
  }

  /**
   * Loads an entity from the entity store by its name identifier.
   *
   * @param ident The {@link NameIdentifier} of the entity to load.
   * @param type The type of the entity to load.
   * @return The loaded entity, or null if it was not found.
   * @throws IOException If an error occurs while loading the entity.
   */
  protected Entity loadMetadataFromDB(NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    Preconditions.checkNotNull(entityStore, NULL_ENTITY_STORE);
    return entityStore.get(ident, type, getEntityClass(type));
  }

  /**
   * Removes an expired entity from the data cache.
   *
   * @param entity The expired entity to remove.
   */
  protected abstract void invalidateExpiredItemByMetadata(Entity entity);
}
