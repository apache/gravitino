/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;

public class JDBCCatalogPropertyConverter extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the Trino property, value is the Gravitino property
              .put("connection-url", "jdbc-url")
              .put("connection-user", "jdbc-user")
              .put("connection-password", "jdbc-password")
              .build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }
}
