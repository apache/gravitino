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

import static org.apache.gravitino.Entity.EntityType.CATALOG;
import static org.apache.gravitino.Entity.EntityType.METALAKE;
import static org.apache.gravitino.Entity.EntityType.MODEL;
import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.Entity.EntityType.TAG;
import static org.apache.gravitino.Entity.EntityType.TOPIC;

import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
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
    if (type == null) {
      throw new IllegalArgumentException("EntityType must not be null");
    }
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
   * Returns the {@link NameIdentifier} of the entity based on its type.
   *
   * @param entity The entity
   * @return The {@link NameIdentifier} of the entity
   */
  public static NameIdentifier getIdentFromEntity(Entity entity) {
    if (entity instanceof HasIdentifier) {
      HasIdentifier hasIdentifier = (HasIdentifier) entity;
      return hasIdentifier.nameIdentifier();
    }

    throw new IllegalArgumentException("Unsupported EntityType: " + entity.type().getShortName());
  }

  /**
   * Returns the id of the entity based on its type.
   *
   * @param entity The entity
   * @return The id of the entity
   */
  public static long getIdFromEntity(Entity entity) {
    if (entity instanceof HasIdentifier) {
      HasIdentifier hasIdentifier = (HasIdentifier) entity;
      return hasIdentifier.id();
    }

    throw new IllegalArgumentException("Unsupported EntityType: " + entity.type().getShortName());
  }

  /**
   * Returns the namespace from ModelPO.
   *
   * @param modelPO The {@link ModelPO} instance.
   * @return The namespace of the model
   */
  public static Namespace getNamespaceFromModel(ModelPO modelPO) {
    Long metalakeId = modelPO.getMetalakeId();
    Long catalogId = modelPO.getCatalogId();
    Long schemaId = modelPO.getSchemaId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    String catalogName =
        CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    String schemaName = SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();

    return Namespace.of(metalakeName, catalogName, schemaName);
  }

  /**
   * Returns the namespace from TablePO.
   *
   * @param tablePO The {@link TablePO} instance.
   * @return The namespace of the table
   */
  public static Namespace getNamespaceFromTable(TablePO tablePO) {
    Long metalakeId = tablePO.getMetalakeId();
    Long catalogId = tablePO.getCatalogId();
    Long schemaId = tablePO.getSchemaId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    String catalogName =
        CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    String schemaName = SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();

    return Namespace.of(metalakeName, catalogName, schemaName);
  }

  /**
   * Returns the namespace from FilesetPO.
   *
   * @param filesetPO The {@link FilesetPO} instance.
   * @return The namespace of the fileset
   */
  public static Namespace getNamespaceFromFileset(FilesetPO filesetPO) {
    Long metalakeId = filesetPO.getMetalakeId();
    Long catalogId = filesetPO.getCatalogId();
    Long schemaId = filesetPO.getSchemaId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    String catalogName =
        CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    String schemaName = SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();

    return Namespace.of(metalakeName, catalogName, schemaName);
  }

  /**
   * Returns the namespace from TopicPO.
   *
   * @param topicPO The {@link TopicPO} instance.
   * @return The namespace of the topic
   */
  public static Namespace getNameSpaceFromTopic(TopicPO topicPO) {
    Long metalakeId = topicPO.getMetalakeId();
    Long catalogId = topicPO.getCatalogId();
    Long schemaId = topicPO.getSchemaId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    String catalogName =
        CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();
    String schemaName = SchemaMetaService.getInstance().getSchemaPOById(schemaId).getSchemaName();

    return Namespace.of(metalakeName, catalogName, schemaName);
  }

  /**
   * Returns the namespace from SchemaPO.
   *
   * @param schemaPO The {@link SchemaPO} instance.
   * @return The namespace of the schema
   */
  public static Namespace getNamespaceFromSchema(SchemaPO schemaPO) {
    Long metalakeId = schemaPO.getMetalakeId();
    Long catalogId = schemaPO.getCatalogId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();
    String catalogName =
        CatalogMetaService.getInstance().getCatalogPOById(catalogId).getCatalogName();

    return Namespace.of(metalakeName, catalogName);
  }

  /**
   * Returns the namespace from CatalogPO.
   *
   * @param catalogPO The {@link CatalogPO} instance.
   * @return The namespace of the catalog
   */
  public static Namespace getNamespaceFromCatalog(CatalogPO catalogPO) {
    Long metalakeId = catalogPO.getMetalakeId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();

    return Namespace.of(metalakeName);
  }

  /**
   * Returns the namespace from TagPO.
   *
   * @param tagPO The {@link TagPO} instance.
   * @return The namespace of the tag
   */
  public static Namespace getNamespaceFromTag(TagPO tagPO) {
    Long metalakeId = tagPO.getMetalakeId();

    String metalakeName =
        MetalakeMetaService.getInstance().getMetalakePOById(metalakeId).getMetalakeName();

    return NamespaceUtil.ofTag(metalakeName);
  }
}
