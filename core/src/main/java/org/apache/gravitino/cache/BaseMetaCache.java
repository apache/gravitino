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
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.FilesetMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.ModelMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.storage.relational.service.TopicMetaService;

/**
 * An abstract class that provides a basic implementation for the MetaCache interface. This class is
 * abstract and cannot be instantiated directly, it is designed to be a base class for other meta
 * cache implementations.
 *
 * <p>The purpose of the BaseMetaCache is to provide a unified way of accessing entity stores,
 * allowing subclasses to focus on caching logic without having to deal with entity store
 * management.
 */
public abstract class BaseMetaCache implements MetaCache {
  // The entity store used by the cache, initialized through the constructor.
  protected final EntityStore entityStore;
  protected final CacheConfig cacheConfig;

  /**
   * Constructs a new {@link BaseMetaCache} instance. If the provided entityStore is null, it will
   * use the entity store configured in the Gravitino environment.
   *
   * @param entityStore The entity store to be used by the cache, can be null.
   */
  public BaseMetaCache(CacheConfig cacheConfig, EntityStore entityStore) {
    if (entityStore == null) {
      entityStore = GravitinoEnv.getInstance().entityStore();
    }

    this.cacheConfig = cacheConfig;
    this.entityStore = entityStore;
  }

  /**
   * Loads an entity from the entity store by its id.
   *
   * @param id The id of the entity to load.
   * @param type The type of the entity to load.
   * @return The loaded entity, or null if it was not found.
   */
  protected Entity loadMetadataFromDBById(Long id, Entity.EntityType type) {
    switch (type) {
      case METALAKE:
        return MetalakeMetaService.getInstance().getMetalakeById(id);

      case CATALOG:
        CatalogPO catalogPO = CatalogMetaService.getInstance().getCatalogPOById(id);
        return CacheUtils.getCatalogEntityFromPO(this, catalogPO);

      case SCHEMA:
        SchemaPO schemaPO = SchemaMetaService.getInstance().getSchemaPOById(id);
        return CacheUtils.getSchemaEntityFromPO(this, schemaPO);

      case TOPIC:
        TopicPO topicPO = TopicMetaService.getInstance().getTopicPOById(id);
        return CacheUtils.getTopicEntityFromPO(this, topicPO);

      case FILESET:
        FilesetPO filesetPO = FilesetMetaService.getInstance().getFilesetPOById(id);
        return CacheUtils.getFilesetEntityFromPO(this, filesetPO);

      case MODEL:
        ModelPO modelPO = ModelMetaService.getInstance().getModelPOById(id);
        return CacheUtils.getModelEntityFromPO(this, modelPO);

      case TABLE:
        TablePO tablePO = TableMetaService.getInstance().getTablePOById(id);
        return CacheUtils.getTableEntityFromPO(this, tablePO);

      case TAG:
        TagPO tagPO = TagMetaService.getInstance().getTagPOByID(id);
        return CacheUtils.getTagEntityFromPO(this, tagPO);

      default:
        throw new IllegalArgumentException("Unsupported entity type: " + type);
    }
  }

  /**
   * Loads an entity from the entity store by its name identifier.
   *
   * @param ident The {@link NameIdentifier} of the entity to load.
   * @param type The type of the entity to load.
   * @return The loaded entity, or null if it was not found.
   * @throws IOException If an error occurs while loading the entity.
   */
  protected Entity loadMetadataFromDBByName(NameIdentifier ident, Entity.EntityType type)
      throws IOException {
    return entityStore.get(ident, type, CacheUtils.getEntityClass(type));
  }

  /**
   * Removes an expired entity from the data cache.
   *
   * @param entity The expired entity to remove.
   */
  protected abstract void removeExpiredMetadataFromDataCache(Entity entity);
}
