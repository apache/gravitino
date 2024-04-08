/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;

class NameIdentifierUtils {

  private NameIdentifierUtils() {}

  public static NameIdentifier ofUser(String metalake, String user) {
    return NameIdentifier.of(
        metalake, CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME, SchemaEntity.USER_SCHEMA_NAME, user);
  }

  public static NameIdentifier ofGroup(String metalake, String group) {
    return NameIdentifier.of(
        metalake,
        CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME,
        SchemaEntity.GROUP_SCHEMA_NAME,
        group);
  }

  public static NameIdentifier ofRole(String metalake, String role) {
    return NameIdentifier.of(
        metalake, CatalogEntity.SYSTEM_CATALOG_RESERVED_NAME, SchemaEntity.ROLE_SCHEMA_NAME, role);
  }
}
