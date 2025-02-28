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

import com.google.common.base.Preconditions;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.storage.relational.helper.CatalogIds;
import org.apache.gravitino.storage.relational.helper.SchemaIds;

/** The service class for common metadata operations. */
public class CommonMetaService {
  private static final CommonMetaService INSTANCE = new CommonMetaService();

  public static CommonMetaService getInstance() {
    return INSTANCE;
  }

  private CommonMetaService() {}

  public Long getParentEntityIdByNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !namespace.isEmpty() && namespace.levels().length <= 3,
        "Namespace should not be empty and length should be less than or equal to 3.");

    Long parentEntityId = null;
    if (namespace.levels().length == 1) {
      parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(namespace.level(0));
    }

    if (namespace.levels().length == 2) {
      parentEntityId =
          CatalogMetaService.getInstance()
              .getCatalogIdByMetalakeAndCatalogName(namespace.level(0), namespace.level(1))
              .getCatalogId();
    }

    if (namespace.levels().length == 3) {
      parentEntityId =
          SchemaMetaService.getInstance()
              .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                  namespace.level(0), namespace.level(1), namespace.level(2))
              .getSchemaId();
    }

    Preconditions.checkState(
        parentEntityId != null && parentEntityId > 0,
        "Parent entity id should not be null and should be greater than 0.");
    return parentEntityId;
  }

  public Long[] getParentEntityIdsByNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !namespace.isEmpty() && namespace.levels().length <= 3,
        "Namespace should not be empty and length should be less than or equal to 3.");
    Long[] parentEntityIds = new Long[namespace.levels().length];

    if (namespace.levels().length == 1) {
      parentEntityIds[0] =
          MetalakeMetaService.getInstance().getMetalakeIdByName(namespace.level(0));
    } else if (namespace.levels().length == 2) {
      CatalogIds catalogIds =
          CatalogMetaService.getInstance()
              .getCatalogIdByMetalakeAndCatalogName(namespace.level(0), namespace.level(1));
      parentEntityIds[0] = catalogIds.getMetalakeId();
      parentEntityIds[1] = catalogIds.getCatalogId();
    } else if (namespace.levels().length == 3) {
      SchemaIds schemaIds =
          SchemaMetaService.getInstance()
              .getSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
                  namespace.level(0), namespace.level(1), namespace.level(2));
      parentEntityIds[0] = schemaIds.getMetalakeId();
      parentEntityIds[1] = schemaIds.getCatalogId();
      parentEntityIds[2] = schemaIds.getSchemaId();
    }

    return parentEntityIds;
  }
}
