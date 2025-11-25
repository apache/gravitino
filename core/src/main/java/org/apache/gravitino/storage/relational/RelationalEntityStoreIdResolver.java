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
package org.apache.gravitino.storage.relational;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.EntityIdResolver;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.service.CatalogMetaService;
import org.apache.gravitino.storage.relational.service.FilesetMetaService;
import org.apache.gravitino.storage.relational.service.GroupMetaService;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.ModelMetaService;
import org.apache.gravitino.storage.relational.service.RoleMetaService;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.storage.relational.service.TableColumnMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.storage.relational.service.TagMetaService;
import org.apache.gravitino.storage.relational.service.TopicMetaService;
import org.apache.gravitino.storage.relational.service.UserMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class RelationalEntityStoreIdResolver implements EntityIdResolver {
  private static final Set<Entity.EntityType> ENTITY_TYPES_REQUIRING_METALAKE_ID =
      ImmutableSet.of(
          Entity.EntityType.METALAKE,
          Entity.EntityType.ROLE,
          Entity.EntityType.USER,
          Entity.EntityType.GROUP,
          Entity.EntityType.TAG);
  private static final Set<Entity.EntityType> ENTITY_TYPES_REQURING_CATALOG_IDS =
      ImmutableSet.of(Entity.EntityType.CATALOG);
  private static final Set<Entity.EntityType> ENTITY_TYPES_REQURING_SCHEMA_IDS =
      ImmutableSet.of(
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.FILESET,
          Entity.EntityType.TOPIC,
          Entity.EntityType.MODEL,
          Entity.EntityType.COLUMN);

  @Override
  public NamespacedEntityId getEntityIds(NameIdentifier nameIdentifier, Entity.EntityType type) {
    if (ENTITY_TYPES_REQUIRING_METALAKE_ID.contains(type)) {
      return getEntityIdsRequiringMetalakeId(nameIdentifier, type);

    } else if (ENTITY_TYPES_REQURING_CATALOG_IDS.contains(type)) {
      CatalogIds catalogIds =
          CatalogMetaService.getInstance()
              .getCatalogIdByMetalakeAndCatalogName(
                  NameIdentifierUtil.getMetalake(nameIdentifier),
                  NameIdentifierUtil.getCatalogIdentifier(nameIdentifier).name());
      return new NamespacedEntityId(catalogIds.getCatalogId(), catalogIds.getMetalakeId());

    } else if (ENTITY_TYPES_REQURING_SCHEMA_IDS.contains(type)) {
      return getEntityIdsRequiringSchemaIds(nameIdentifier, type);

    } else {
      throw new IllegalArgumentException("Unsupported entity type: " + type);
    }
  }

  @Override
  public Long getEntityId(NameIdentifier nameIdentifier, Entity.EntityType type) {
    if (ENTITY_TYPES_REQUIRING_METALAKE_ID.contains(type)) {
      return getEntityIdsRequiringMetalakeId(nameIdentifier, type).entityId();

    } else if (ENTITY_TYPES_REQURING_CATALOG_IDS.contains(type)) {
      return CatalogMetaService.getInstance()
          .getCatalogIdByMetalakeAndCatalogName(
              NameIdentifierUtil.getMetalake(nameIdentifier),
              NameIdentifierUtil.getCatalogIdentifier(nameIdentifier).name())
          .getCatalogId();

    } else if (ENTITY_TYPES_REQURING_SCHEMA_IDS.contains(type)) {
      return getEntityIdsRequiringSchemaIds(nameIdentifier, type).entityId();

    } else {
      throw new IllegalArgumentException("Unsupported entity type: " + type);
    }
  }

  private NamespacedEntityId getEntityIdsRequiringMetalakeId(
      NameIdentifier nameIdentifier, Entity.EntityType type) {
    long metalakeId =
        MetalakeMetaService.getInstance()
            .getMetalakeIdByName(NameIdentifierUtil.getMetalake(nameIdentifier));

    switch (type) {
      case METALAKE:
        return new NamespacedEntityId(metalakeId);

      case ROLE:
        long roleId =
            RoleMetaService.getInstance()
                .getRoleIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
        return new NamespacedEntityId(roleId, metalakeId);

      case USER:
        long userId =
            UserMetaService.getInstance()
                .getUserIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
        return new NamespacedEntityId(userId, metalakeId);

      case GROUP:
        long groupId =
            GroupMetaService.getInstance()
                .getGroupIdByMetalakeIdAndName(metalakeId, nameIdentifier.name());
        return new NamespacedEntityId(groupId, metalakeId);

      case TAG:
        long tagId =
            TagMetaService.getInstance().getTagIdByTagName(metalakeId, nameIdentifier.name());
        return new NamespacedEntityId(tagId, metalakeId);

      default:
        throw new IllegalArgumentException("Unsupported entity type: " + type);
    }
  }

  private NamespacedEntityId getEntityIdsRequiringSchemaIds(
      NameIdentifier nameIdentifier, Entity.EntityType type) {
    SchemaIds schemaIds =
        SchemaMetaService.getInstance()
            .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                NameIdentifierUtil.getMetalake(nameIdentifier),
                NameIdentifierUtil.getCatalogIdentifier(nameIdentifier).name(),
                NameIdentifierUtil.getSchemaIdentifier(nameIdentifier).name());

    switch (type) {
      case SCHEMA:
        return new NamespacedEntityId(
            schemaIds.getSchemaId(), schemaIds.getMetalakeId(), schemaIds.getCatalogId());

      case TABLE:
        long tableId =
            TableMetaService.getInstance()
                .getTableIdBySchemaIdAndName(schemaIds.getSchemaId(), nameIdentifier.name());
        return new NamespacedEntityId(
            tableId, schemaIds.getMetalakeId(), schemaIds.getCatalogId(), schemaIds.getSchemaId());

      case COLUMN:
        long columnTableId =
            TableMetaService.getInstance()
                .getTableIdBySchemaIdAndName(
                    schemaIds.getSchemaId(),
                    NameIdentifier.of(nameIdentifier.namespace().levels()).name());
        long columnId =
            TableColumnMetaService.getInstance()
                .getColumnIdByTableIdAndName(columnTableId, nameIdentifier.name());
        return new NamespacedEntityId(
            columnId,
            schemaIds.getMetalakeId(),
            schemaIds.getCatalogId(),
            schemaIds.getSchemaId(),
            columnTableId);

      case FILESET:
        long filesetId =
            FilesetMetaService.getInstance()
                .getFilesetIdBySchemaIdAndName(schemaIds.getSchemaId(), nameIdentifier.name());
        return new NamespacedEntityId(
            filesetId,
            schemaIds.getMetalakeId(),
            schemaIds.getCatalogId(),
            schemaIds.getSchemaId());

      case TOPIC:
        long topicId =
            TopicMetaService.getInstance()
                .getTopicIdBySchemaIdAndName(schemaIds.getSchemaId(), nameIdentifier.name());
        return new NamespacedEntityId(
            topicId, schemaIds.getMetalakeId(), schemaIds.getCatalogId(), schemaIds.getSchemaId());

      case MODEL:
        long modelId =
            ModelMetaService.getInstance()
                .getModelIdBySchemaIdAndModelName(schemaIds.getSchemaId(), nameIdentifier.name());
        return new NamespacedEntityId(
            modelId, schemaIds.getMetalakeId(), schemaIds.getCatalogId(), schemaIds.getSchemaId());

      default:
        throw new IllegalArgumentException("Unsupported entity type: " + type);
    }
  }
}
