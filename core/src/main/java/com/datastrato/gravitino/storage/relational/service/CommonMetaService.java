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

package com.datastrato.gravitino.storage.relational.service;

import com.apache.gravitino.Namespace;
import com.google.common.base.Preconditions;

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
    for (int level = 0; level < namespace.levels().length; level++) {
      String name = namespace.level(level);
      switch (level) {
        case 0:
          parentEntityId = MetalakeMetaService.getInstance().getMetalakeIdByName(name);
          continue;
        case 1:
          parentEntityId =
              CatalogMetaService.getInstance()
                  .getCatalogIdByMetalakeIdAndName(parentEntityId, name);
          continue;
        case 2:
          parentEntityId =
              SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(parentEntityId, name);
          break;
      }
    }
    Preconditions.checkState(
        parentEntityId != null && parentEntityId > 0,
        "Parent entity id should not be null and should be greater than 0.");
    return parentEntityId;
  }
}
