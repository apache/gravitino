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

package org.apache.gravitino.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;

/**
 * Utility class that provides mapping between entity types and their corresponding entity classes.
 */
public class EntityClassMapper {

  /** Maps entity type to entity class. */
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
          .put(Entity.EntityType.GROUP, GroupEntity.class)
          .put(Entity.EntityType.ROLE, RoleEntity.class)
          .put(Entity.EntityType.POLICY, PolicyEntity.class)
          .put(Entity.EntityType.JOB_TEMPLATE, JobTemplateEntity.class)
          .put(Entity.EntityType.JOB, JobEntity.class)
          .build();

  private EntityClassMapper() {}

  /**
   * Get the entity class for the given entity type.
   *
   * @param entityType the entity type
   * @param <E> the entity class type that extends Entity and HasIdentifier
   * @return the entity class, or null if not found
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> Class<E> getEntityClass(
      Entity.EntityType entityType) {
    Class<?> clazz = ENTITY_CLASS_MAPPING.get(entityType);
    return (Class<E>) clazz;
  }
}
