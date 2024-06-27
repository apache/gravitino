/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
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

    String[] level = namespace.levels();
    NameIdentifier ident = NameIdentifier.of(level);
    Long entityId = null;

    switch (level.length) {
      case 1:
        entityId = MetalakeMetaService.getInstance().getMetalakeIdByNameIdentifier(ident);
        break;
      case 2:
        entityId = CatalogMetaService.getInstance().getCatalogIdByNameIdentifier(ident);
        break;
      case 3:
        entityId = SchemaMetaService.getInstance().getSchemaIdByNameIdentifier(ident);
        break;
    }

    Preconditions.checkState(
        entityId != null && entityId > 0,
        "Parent entity id should not be null and should be greater than 0.");
    return entityId;
  }
}
