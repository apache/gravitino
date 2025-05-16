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
import org.apache.gravitino.GravitinoEnv;
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
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.utils.NamespaceUtil;

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
  private static final Map<Entity.EntityType, Class<?>> ENTITY_CLASS_MAP;
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
   * Returns the id of the metadata based on its type.
   *
   * @param metadata The metadata
   * @return The id of the metadata
   */
  public static long getIdFromMetadata(Entity metadata) {
    if (metadata instanceof HasIdentifier) {
      HasIdentifier hasIdentifier = (HasIdentifier) metadata;
      return hasIdentifier.id();
    }

    throw new IllegalArgumentException("Unsupported EntityType: " + metadata.type().getShortName());
  }

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
        return getCatalogEntityFromPO(catalogPO);

      case SCHEMA:
        SchemaPO schemaPO = SchemaMetaService.getInstance().getSchemaPOById(id);
        return getSchemaEntityFromPO(schemaPO);

      case TOPIC:
        TopicPO topicPO = TopicMetaService.getInstance().getTopicPOById(id);
        return getTopicEntityFromPO(topicPO);

      case FILESET:
        FilesetPO filesetPO = FilesetMetaService.getInstance().getFilesetPOById(id);
        return getFilesetEntityFromPO(filesetPO);

      case MODEL:
        ModelPO modelPO = ModelMetaService.getInstance().getModelPOById(id);
        return getModelEntityFromPO(modelPO);

      case TABLE:
        TablePO tablePO = TableMetaService.getInstance().getTablePOById(id);
        return getTableEntityFromPO(tablePO);

      case TAG:
        TagPO tagPO = TagMetaService.getInstance().getTagPOByID(id);
        return getTagEntityFromPO(tagPO);

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
    return entityStore.get(ident, type, getEntityClass(type));
  }

  /**
   * Removes an expired entity from the data cache.
   *
   * @param entity The expired entity to remove.
   */
  protected abstract void removeExpiredMetadataFromDataCache(Entity entity);

  /**
   * Returns the entity from the PO based on its type.
   *
   * @param catalogPO The catalog PO to convert to an entity.
   * @return The {@link CatalogEntity} instance.
   */
  private CatalogEntity getCatalogEntityFromPO(CatalogPO catalogPO) {
    String metalakeName = getMetalakeName(catalogPO.getMetalakeId());

    return POConverters.fromCatalogPO(catalogPO, NamespaceUtil.ofCatalog(metalakeName));
  }

  /**
   * Returns the schema entity from the schema PO.
   *
   * @param schemaPO The schema PO to convert to an entity.
   * @return The {@link SchemaEntity} instance.
   */
  private SchemaEntity getSchemaEntityFromPO(SchemaPO schemaPO) {
    String metalakeName = getMetalakeName(schemaPO.getMetalakeId());
    String catalogName = getCatalogName(schemaPO.getCatalogId());

    return POConverters.fromSchemaPO(schemaPO, NamespaceUtil.ofSchema(metalakeName, catalogName));
  }

  /**
   * Returns the table entity from the table PO.
   *
   * @param tablePO The table PO to convert to an entity.
   * @return The {@link TableEntity} instance.
   */
  private TableEntity getTableEntityFromPO(TablePO tablePO) {
    String metalakeName = getMetalakeName(tablePO.getMetalakeId());
    String catalogName = getCatalogName(tablePO.getCatalogId());
    String schemaName = getSchemaName(tablePO.getSchemaId());

    return POConverters.fromTablePO(
        tablePO, NamespaceUtil.ofTable(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the model entity from the model PO.
   *
   * @param modelPO The model PO to convert to an entity.
   * @return The {@link ModelEntity} instance.
   */
  private ModelEntity getModelEntityFromPO(ModelPO modelPO) {
    String metalakeName = getMetalakeName(modelPO.getMetalakeId());
    String catalogName = getCatalogName(modelPO.getCatalogId());
    String schemaName = getSchemaName(modelPO.getSchemaId());

    return POConverters.fromModelPO(
        modelPO, NamespaceUtil.ofModel(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the topic entity from the topic PO.
   *
   * @param topicPO The topic PO to convert to an entity.
   * @return The {@link TopicEntity} instance.
   */
  private TopicEntity getTopicEntityFromPO(TopicPO topicPO) {
    String metalakeName = getMetalakeName(topicPO.getMetalakeId());
    String catalogName = getCatalogName(topicPO.getCatalogId());
    String schemaName = getSchemaName(topicPO.getSchemaId());

    return POConverters.fromTopicPO(
        topicPO, NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the fileset entity from the fileset PO.
   *
   * @param filesetPO The fileset PO to convert to an entity.
   * @return The {@link FilesetEntity} instance.
   */
  private FilesetEntity getFilesetEntityFromPO(FilesetPO filesetPO) {
    String metalakeName = getMetalakeName(filesetPO.getMetalakeId());
    String catalogName = getCatalogName(filesetPO.getCatalogId());
    String schemaName = getSchemaName(filesetPO.getSchemaId());

    return POConverters.fromFilesetPO(
        filesetPO, NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the tag entity from the tag PO.
   *
   * @param tagPO The tag PO to convert to an entity.
   * @return The {@link TagEntity} instance.
   */
  private TagEntity getTagEntityFromPO(TagPO tagPO) {
    String metalakeName = getMetalakeName(tagPO.getMetalakeId());

    return POConverters.fromTagPO(tagPO, NamespaceUtil.ofTag(metalakeName));
  }

  /**
   * Returns the name of the metalake based on its id.
   *
   * @param metalakeId The id of the metalake.
   * @return if the metalake is in the cache, returns the name of the metalake. Otherwise, returns
   *     the name of the metalake from the database.
   */
  private String getMetalakeName(Long metalakeId) {
    if (containsById(metalakeId, Entity.EntityType.METALAKE)) {
      return getIdentFromMetadata(getOrLoadMetadataById(metalakeId, Entity.EntityType.METALAKE))
          .name();
    } else {
      return MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    }
  }

  /**
   * Returns the name of the catalog based on its id.
   *
   * @param catalogId The id of the catalog.
   * @return if the catalog is in the cache, returns the name of the catalog. Otherwise, returns the
   *     name of the catalog from the database.
   */
  private String getCatalogName(Long catalogId) {
    if (containsById(catalogId, Entity.EntityType.CATALOG)) {
      return getIdentFromMetadata(getOrLoadMetadataById(catalogId, Entity.EntityType.CATALOG))
          .name();
    } else {
      return CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    }
  }

  /**
   * Returns the name of the schema based on its id.
   *
   * @param schemaId The id of the schema.
   * @return if the schema is in the cache, returns the name of the schema. Otherwise, returns the
   *     name of the schema from the database.
   */
  private String getSchemaName(Long schemaId) {
    if (containsById(schemaId, Entity.EntityType.SCHEMA)) {
      return getIdentFromMetadata(getOrLoadMetadataById(schemaId, Entity.EntityType.SCHEMA)).name();
    } else {
      return SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();
    }
  }
}
