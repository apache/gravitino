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

package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.helper.SchemaIds;

/** The service class for common metadata operations. */
public class CommonMetaService {
  private static final CommonMetaService INSTANCE = new CommonMetaService();

  public static CommonMetaService getInstance() {
    return INSTANCE;
  }

  private CommonMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getParentEntityIdByNamespace")
  public Long getParentEntityIdByNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !namespace.isEmpty() && namespace.levels().length <= 3,
        "Namespace should not be empty and length should be less than or equal to 3.");

    int length = namespace.levels().length;
    Long parentEntityId;
    switch (length) {
      case 1:
        // Parent is a metalake
        parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(namespace.level(0));
        if (parentEntityId == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.METALAKE.name().toLowerCase(),
              namespace);
        }

        return parentEntityId;
      case 2:
        // Parent is a catalog
        CatalogIds catalogIds =
            CatalogMetaService.getInstance()
                .getCatalogIdByMetalakeAndCatalogName(namespace.level(0), namespace.level(1));
        parentEntityId = catalogIds == null ? null : catalogIds.getCatalogId();
        if (parentEntityId == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.CATALOG.name().toLowerCase(),
              namespace);
        }

        return parentEntityId;
      case 3:
        // Parent is a schema
        SchemaIds schemaIds =
            SchemaMetaService.getInstance()
                .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                    namespace.level(0), namespace.level(1), namespace.level(2));
        parentEntityId = schemaIds == null ? null : schemaIds.getSchemaId();
        if (parentEntityId == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.SCHEMA.name().toLowerCase(),
              namespace);
        }
        return parentEntityId;
      default:
        throw new IllegalArgumentException("Namespace length should be less than or equal to 3.");
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getParentEntityIdsByNamespace")
  public Long[] getParentEntityIdsByNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !namespace.isEmpty() && namespace.levels().length <= 3,
        "Namespace should not be empty and length should be less than or equal to 3.");
    Long[] parentEntityIds = new Long[namespace.levels().length];

    int length = namespace.levels().length;
    switch (length) {
      case 1:
        // Parent is a metalake
        parentEntityIds[0] =
            MetalakeMetaService.getInstance().getMetalakeIdByName(namespace.level(0));
        if (parentEntityIds[0] == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.METALAKE.name().toLowerCase(),
              namespace);
        }

        return parentEntityIds;
      case 2:
        // Parent is a catalog
        CatalogIds catalogIds =
            CatalogMetaService.getInstance()
                .getCatalogIdByMetalakeAndCatalogName(namespace.level(0), namespace.level(1));
        parentEntityIds[0] = catalogIds == null ? null : catalogIds.getMetalakeId();
        parentEntityIds[1] = catalogIds == null ? null : catalogIds.getCatalogId();

        if (parentEntityIds[1] == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.CATALOG.name().toLowerCase(),
              namespace);
        }
        return parentEntityIds;
      case 3:
        // Parent is a schema
        SchemaIds schemaIds =
            SchemaMetaService.getInstance()
                .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                    namespace.level(0), namespace.level(1), namespace.level(2));
        parentEntityIds[0] = schemaIds == null ? null : schemaIds.getMetalakeId();
        parentEntityIds[1] = schemaIds == null ? null : schemaIds.getCatalogId();
        parentEntityIds[2] = schemaIds == null ? null : schemaIds.getSchemaId();

        if (parentEntityIds[2] == null) {
          throw new NoSuchEntityException(
              NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
              Entity.EntityType.SCHEMA.name().toLowerCase(),
              namespace);
        }

        return parentEntityIds;
      default:
        throw new IllegalArgumentException("Namespace length should be less than or equal to 3.");
    }
  }
}
