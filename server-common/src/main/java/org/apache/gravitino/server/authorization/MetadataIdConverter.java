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

package org.apache.gravitino.server.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
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

/** It is used to convert MetadataObject to MetadataId */
public class MetadataIdConverter {
  private static final Pattern DOT_PATTERN = Pattern.compile("\\.");
  // Maps metadata type to entity type
  private static final Map<MetadataObject.Type, Entity.EntityType> METADATA_TO_ENTITY_TYPE_MAPPING =
      ImmutableMap.of(
          MetadataObject.Type.METALAKE, Entity.EntityType.METALAKE,
          MetadataObject.Type.CATALOG, Entity.EntityType.CATALOG,
          MetadataObject.Type.SCHEMA, Entity.EntityType.SCHEMA,
          MetadataObject.Type.TABLE, Entity.EntityType.TABLE,
          MetadataObject.Type.MODEL, Entity.EntityType.MODEL,
          MetadataObject.Type.FILESET, Entity.EntityType.FILESET,
          MetadataObject.Type.TOPIC, Entity.EntityType.TOPIC,
          MetadataObject.Type.COLUMN, Entity.EntityType.COLUMN,
          MetadataObject.Type.ROLE, Entity.EntityType.ROLE);
  // Maps metadata type to capability scope
  private static final Map<MetadataObject.Type, Capability.Scope> METADATA_SCOPE_MAPPING =
      ImmutableMap.of(
          MetadataObject.Type.SCHEMA, Capability.Scope.SCHEMA,
          MetadataObject.Type.TABLE, Capability.Scope.TABLE,
          MetadataObject.Type.MODEL, Capability.Scope.MODEL,
          MetadataObject.Type.FILESET, Capability.Scope.FILESET,
          MetadataObject.Type.TOPIC, Capability.Scope.TOPIC,
          MetadataObject.Type.COLUMN, Capability.Scope.COLUMN);
  // Maps entity type to entity class.
  private static final Map<Entity.EntityType, Class<?>> ENTITY_CLASS_MAPPING =
      ImmutableMap.<Entity.EntityType, Class<?>>builder()
          .put(Entity.EntityType.METALAKE, BaseMetalake.class)
          .put(Entity.EntityType.CATALOG, CatalogEntity.class)
          .put(Entity.EntityType.SCHEMA, SchemaEntity.class)
          .put(Entity.EntityType.TABLE, TableEntity.class)
          .put(Entity.EntityType.FILESET, FilesetEntity.class)
          .put(Entity.EntityType.MODEL, ModelEntity.class)
          .put(Entity.EntityType.TOPIC, TopicEntity.class)
          .put(Entity.EntityType.TAG, TagEntity.class)
          .put(Entity.EntityType.MODEL_VERSION, ModelVersionEntity.class)
          .put(Entity.EntityType.COLUMN, ColumnEntity.class)
          .put(Entity.EntityType.USER, UserEntity.class)
          .put(Entity.EntityType.GROUP, Entity.class)
          .put(Entity.EntityType.ROLE, RoleEntity.class)
          .build();

  private MetadataIdConverter() {}

  /**
   * Converts the given metadata object to metadata id.
   *
   * @param metadataObject The metadata object to convert.
   * @param metalake The metalake name.
   * @return The metadata id.
   * @throws IOException if an error occurs while loading the entity.
   */
  public static Long getID(MetadataObject metadataObject, String metalake) throws IOException {
    Preconditions.checkArgument(metadataObject != null, "Metadata object cannot be null");
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();

    MetadataObject.Type metadataType = metadataObject.type();
    NameIdentifier ident =
        (metadataType != MetadataObject.Type.METALAKE)
            ? NameIdentifier.of(DOT_PATTERN.split(metalake + "." + metadataObject.fullName()))
            : NameIdentifier.of(metadataObject.fullName());

    NameIdentifier normalizedIdent =
        normalizeCaseSensitive(ident, METADATA_SCOPE_MAPPING.get(metadataType), catalogManager);

    Entity.EntityType entityType = getEntityType(metadataType);

    Entity entity;
    try {
      entity = entityStore.get(normalizedIdent, entityType, getEntityClass(entityType));
    } catch (IOException e) {
      throw new IOException(
          "failed to load entity from entity store: " + metadataObject.fullName(), e);
    }

    return extractIdFromEntity(entity);
  }

  @VisibleForTesting
  static NameIdentifier normalizeCaseSensitive(
      NameIdentifier ident, Capability.Scope scope, CatalogManager catalogManager) {
    if (scope == null) {
      return ident;
    }

    Capability capability = CapabilityHelpers.getCapability(ident, catalogManager);
    return CapabilityHelpers.applyCaseSensitive(ident, scope, capability);
  }

  private static Entity.EntityType getEntityType(MetadataObject.Type metadataType) {
    return METADATA_TO_ENTITY_TYPE_MAPPING.get(metadataType);
  }

  private static Long extractIdFromEntity(Entity entity) {
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Entity must implement HasIdentifier interface");

    return ((HasIdentifier) entity).id();
  }

  @SuppressWarnings("unchecked")
  private static <E extends Entity & HasIdentifier> Class<E> getEntityClass(
      Entity.EntityType entityType) {
    Class<?> clazz = ENTITY_CLASS_MAPPING.get(entityType);
    return (Class<E>) clazz;
  }
}
