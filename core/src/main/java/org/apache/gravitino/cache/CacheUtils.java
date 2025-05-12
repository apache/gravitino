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
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.utils.NamespaceUtil;

public class CacheUtils {

  /**
   * Returns the class of the entity based on its type.
   *
   * @param type The entity type
   * @return The class of the entity
   * @throws IllegalArgumentException if the entity type is not supported
   */
  public static <E extends Entity & HasIdentifier> Class<E> getEntityClass(Entity.EntityType type) {
    Preconditions.checkNotNull(type, "EntityType must not be null");

    switch (type) {
      case METALAKE:
        return (Class<E>) BaseMetalake.class;

      case CATALOG:
        return (Class<E>) CatalogEntity.class;

      case SCHEMA:
        return (Class<E>) SchemaEntity.class;

      case TABLE:
        return (Class<E>) TableEntity.class;

      case FILESET:
        return (Class<E>) FilesetEntity.class;

      case MODEL:
        return (Class<E>) ModelEntity.class;

      case TOPIC:
        return (Class<E>) TopicEntity.class;

      case TAG:
        return (Class<E>) TagEntity.class;

      default:
        throw new IllegalArgumentException("Unsupported EntityType: " + type.getShortName());
    }
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
   * Returns the entity from the PO based on its type.
   *
   * @param cache The cache to retrieve the entity from.
   * @param catalogPO The catalog PO to convert to an entity.
   * @return The {@link CatalogEntity} instance.
   */
  public static CatalogEntity getCatalogEntityFromPO(MetaCache cache, CatalogPO catalogPO) {
    String metalakeName = getMetalakeName(cache, catalogPO.getMetalakeId());

    return POConverters.fromCatalogPO(catalogPO, NamespaceUtil.ofCatalog(metalakeName));
  }

  /**
   * Returns the schema entity from the schema PO.
   *
   * @param cache The cache to retrieve the schema entity from.
   * @param schemaPO The schema PO to convert to an entity.
   * @return The {@link SchemaEntity} instance.
   */
  public static SchemaEntity getSchemaEntityFromPO(MetaCache cache, SchemaPO schemaPO) {
    String metalakeName = getMetalakeName(cache, schemaPO.getMetalakeId());
    String catalogName = getCatalogName(cache, schemaPO.getCatalogId());

    return POConverters.fromSchemaPO(schemaPO, NamespaceUtil.ofSchema(metalakeName, catalogName));
  }

  /**
   * Returns the table entity from the table PO.
   *
   * @param cache The cache to retrieve the table entity from.
   * @param tablePO The table PO to convert to an entity.
   * @return The {@link TableEntity} instance.
   */
  public static TableEntity getTableEntityFromPO(MetaCache cache, TablePO tablePO) {
    String metalakeName = getMetalakeName(cache, tablePO.getMetalakeId());
    String catalogName = getCatalogName(cache, tablePO.getCatalogId());
    String schemaName = getSchemaName(cache, tablePO.getSchemaId());

    return POConverters.fromTablePO(
        tablePO, NamespaceUtil.ofTable(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the model entity from the model PO.
   *
   * @param cache The cache to retrieve the model entity from.
   * @param modelPO The model PO to convert to an entity.
   * @return The {@link ModelEntity} instance.
   */
  public static ModelEntity getModelEntityFromPO(MetaCache cache, ModelPO modelPO) {
    String metalakeName = getMetalakeName(cache, modelPO.getMetalakeId());
    String catalogName = getCatalogName(cache, modelPO.getCatalogId());
    String schemaName = getSchemaName(cache, modelPO.getSchemaId());

    return POConverters.fromModelPO(
        modelPO, NamespaceUtil.ofModel(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the topic entity from the topic PO.
   *
   * @param cache The cache to retrieve the topic entity from.
   * @param topicPO The topic PO to convert to an entity.
   * @return The {@link TopicEntity} instance.
   */
  public static TopicEntity getTopicEntityFromPO(MetaCache cache, TopicPO topicPO) {
    String metalakeName = getMetalakeName(cache, topicPO.getMetalakeId());
    String catalogName = getCatalogName(cache, topicPO.getCatalogId());
    String schemaName = getSchemaName(cache, topicPO.getSchemaId());

    return POConverters.fromTopicPO(
        topicPO, NamespaceUtil.ofTopic(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the fileset entity from the fileset PO.
   *
   * @param cache The cache to retrieve the fileset entity from.
   * @param filesetPO The fileset PO to convert to an entity.
   * @return The {@link FilesetEntity} instance.
   */
  public static FilesetEntity getFilesetEntityFromPO(MetaCache cache, FilesetPO filesetPO) {
    String metalakeName = getMetalakeName(cache, filesetPO.getMetalakeId());
    String catalogName = getCatalogName(cache, filesetPO.getCatalogId());
    String schemaName = getSchemaName(cache, filesetPO.getSchemaId());

    return POConverters.fromFilesetPO(
        filesetPO, NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName));
  }

  /**
   * Returns the tag entity from the tag PO.
   *
   * @param cache The cache to retrieve the tag entity from.
   * @param tagPO The tag PO to convert to an entity.
   * @return The {@link TagEntity} instance.
   */
  public static TagEntity getTagEntityFromPO(MetaCache cache, TagPO tagPO) {
    String metalakeName = getMetalakeName(cache, tagPO.getMetalakeId());

    return POConverters.fromTagPO(tagPO, NamespaceUtil.ofTag(metalakeName));
  }

  /**
   * Returns the name of the metalake based on its id.
   *
   * @param cache The cache to retrieve the metalake name from.
   * @param metalakeId The id of the metalake.
   * @return if the metalake is in the cache, returns the name of the metalake. Otherwise, returns
   *     the name of the metalake from the database.
   */
  public static String getMetalakeName(MetaCache cache, Long metalakeId) {
    if (cache.containsById(metalakeId, Entity.EntityType.METALAKE)) {
      return getIdentFromMetadata(
              cache.getOrLoadMetadataById(metalakeId, Entity.EntityType.METALAKE))
          .name();
    } else {
      return MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    }
  }

  /**
   * Returns the name of the catalog based on its id.
   *
   * @param cache The cache to retrieve the catalog name from.
   * @param catalogId The id of the catalog.
   * @return if the catalog is in the cache, returns the name of the catalog. Otherwise, returns the
   *     name of the catalog from the database.
   */
  public static String getCatalogName(MetaCache cache, Long catalogId) {
    if (cache.containsById(catalogId, Entity.EntityType.CATALOG)) {
      return getIdentFromMetadata(cache.getOrLoadMetadataById(catalogId, Entity.EntityType.CATALOG))
          .name();
    } else {
      return CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    }
  }

  /**
   * Returns the name of the schema based on its id.
   *
   * @param cache The cache to retrieve the schema name from.
   * @param schemaId The id of the schema.
   * @return if the schema is in the cache, returns the name of the schema. Otherwise, returns the
   *     name of the schema from the database.
   */
  public static String getSchemaName(MetaCache cache, Long schemaId) {
    if (cache.containsById(schemaId, Entity.EntityType.SCHEMA)) {
      return getIdentFromMetadata(cache.getOrLoadMetadataById(schemaId, Entity.EntityType.SCHEMA))
          .name();
    } else {
      return SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();
    }
  }
}
