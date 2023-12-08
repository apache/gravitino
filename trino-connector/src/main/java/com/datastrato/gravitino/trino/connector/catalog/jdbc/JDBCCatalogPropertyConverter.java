/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;

public class JDBCCatalogPropertyConverter extends PropertyConverter {

  static final String JDBC_CONNECTION_URL_KEY = "connection-url";
  static final String JDBC_CONNECTION_USER_KEY = "connection-user";
  static final String JDBC_CONNECTION_PASSWORD_KEY = "connection-password";

  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the Trino property, value is the Gravitino property
              .put(JDBC_CONNECTION_URL_KEY, "jdbc-url")
              .put(JDBC_CONNECTION_USER_KEY, "jdbc-user")
              .put(JDBC_CONNECTION_PASSWORD_KEY, "jdbc-password")
              .build());

  public static final Set<String> REQUIRED_PROPERTIES =
      Sets.newHashSet(
          JDBC_CONNECTION_PASSWORD_KEY, JDBC_CONNECTION_USER_KEY, JDBC_CONNECTION_PASSWORD_KEY);

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    Map<String, String> trinoProperties = super.toTrinoProperties(properties);
    for (String requiredProperty : REQUIRED_PROPERTIES) {
      if (!trinoProperties.containsKey(requiredProperty)) {
        throw new IllegalArgumentException("Missing required property: " + requiredProperty);
      }
    }

    return trinoProperties;
  }
}
