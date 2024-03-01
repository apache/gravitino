/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Namespace;
import com.google.common.base.Preconditions;

/** The service class for common metadata operations. */
public class CommonMetaService {
  private static final CommonMetaService INSTANCE = new CommonMetaService();

  public static CommonMetaService getInstance() {
    return INSTANCE;
  }

  private CommonMetaService() {}

  public Long getParentIdByNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        !namespace.isEmpty() && namespace.levels().length <= 3,
        "Parent namespace should not be empty and length should be less and equal than 3");
    Long parentId = null;
    for (int level = 0; level < namespace.levels().length; level++) {
      String name = namespace.level(level);
      switch (level) {
        case 0:
          parentId = MetalakeMetaService.getInstance().getMetalakeIdByName(name);
          continue;
        case 1:
          parentId =
              CatalogMetaService.getInstance().getCatalogIdByMetalakeIdAndName(parentId, name);
          continue;
        case 2:
          parentId = SchemaMetaService.getInstance().getSchemaIdByCatalogIdAndName(parentId, name);
          break;
      }
    }
    Preconditions.checkArgument(
        parentId != null && parentId > 0,
        "Parent id should not be null and should be greater than 0.");
    return parentId;
  }
}
