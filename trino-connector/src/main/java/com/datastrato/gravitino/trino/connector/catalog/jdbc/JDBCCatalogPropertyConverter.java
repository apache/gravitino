/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class JDBCCatalogPropertyConverter extends PropertyConverter {

  static final String JDBC_CONNECTION_URL_KEY = "connection-url";
  static final String JDBC_CONNECTION_USER_KEY = "connection-user";
  static final String JDBC_CONNECTION_PASSWORD_KEY = "connection-password";

  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the Trino property, value is the Gravitino property
              .put(JDBC_CONNECTION_URL_KEY, "jdbc-url")

              // Data source authentication
              .put(JDBC_CONNECTION_USER_KEY, "jdbc-user")
              .put(JDBC_CONNECTION_PASSWORD_KEY, "jdbc-password")
              .put("credential-provider.type", TRINO_PROPERTIES_PREFIX + "credential-provider.type")
              .put("user-credential-name", TRINO_PROPERTIES_PREFIX + "user-credential-name")
              .put("password-credential-name", TRINO_PROPERTIES_PREFIX + "password-credential-name")
              .put(
                  "connection-credential-file",
                  TRINO_PROPERTIES_PREFIX + "connection-credential-file")
              .put("keystore-file-path", TRINO_PROPERTIES_PREFIX + "keystore-file-path")
              .put("keystore-type", TRINO_PROPERTIES_PREFIX + "keystore-type")
              .put("keystore-password", TRINO_PROPERTIES_PREFIX + "keystore-password")
              .put(
                  "keystore-user-credential-name",
                  TRINO_PROPERTIES_PREFIX + "keystore-user-credential-name")
              .put(
                  "keystore-user-credential-password",
                  TRINO_PROPERTIES_PREFIX + "keystore-user-credential-password")
              .put(
                  "keystore-password-credential-name",
                  TRINO_PROPERTIES_PREFIX + "keystore-password-credential-name")
              .put(
                  "keystore-password-credential-password",
                  TRINO_PROPERTIES_PREFIX + "keystore-password-credential-password")

              // General configuration properties
              .put(
                  "case-insensitive-name-matching",
                  TRINO_PROPERTIES_PREFIX + "ase-insensitive-name-matching")
              .put(
                  "case-insensitive-name-matching.cache-ttl",
                  TRINO_PROPERTIES_PREFIX + "case-insensitive-name-matching.cache-ttl")
              .put(
                  "case-insensitive-name-matching.config-file",
                  TRINO_PROPERTIES_PREFIX + "case-insensitive-name-matching.config-file")
              .put(
                  "case-insensitive-name-matching.config-file.refresh-period",
                  TRINO_PROPERTIES_PREFIX
                      + "case-insensitive-name-matching.config-file.refresh-period")
              .put("metadata.cache-ttl", TRINO_PROPERTIES_PREFIX + "metadata.cache-ttl")
              .put("metadata.cache-missing", TRINO_PROPERTIES_PREFIX + "metadata.cache-missing")
              .put(
                  "metadata.schemas.cache-ttl",
                  TRINO_PROPERTIES_PREFIX + "metadata.schemas.cache-ttl")
              .put(
                  "metadata.tables.cache-ttl",
                  TRINO_PROPERTIES_PREFIX + "metadata.tables.cache-ttl")
              .put(
                  "metadata.statistics.cache-ttl",
                  TRINO_PROPERTIES_PREFIX + "metadata.statistics.cache-ttl")
              .put(
                  "metadata.cache-maximum-size",
                  TRINO_PROPERTIES_PREFIX + "metadata.cache-maximum-size")
              .put("write.batch-size", TRINO_PROPERTIES_PREFIX + "write.batch-size")
              .put(
                  "dynamic-filtering.enabled",
                  TRINO_PROPERTIES_PREFIX + "dynamic-filtering.enabled")
              .put(
                  "dynamic-filtering.wait-timeout",
                  TRINO_PROPERTIES_PREFIX + "dynamic-filtering.wait-timeout")

              // Performance
              .put("join-pushdown.enabled", TRINO_PROPERTIES_PREFIX + "join-pushdown.enabled")
              .put("join-pushdown.strategy", TRINO_PROPERTIES_PREFIX + "join-pushdown.strategy")
              .build());

  public static final Set<String> REQUIRED_PROPERTIES =
      Sets.newHashSet(
          JDBC_CONNECTION_PASSWORD_KEY, JDBC_CONNECTION_USER_KEY, JDBC_CONNECTION_PASSWORD_KEY);

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
    Map<String, String> trinoProperties = super.gravitinoToEngineProperties(properties);
    for (String requiredProperty : REQUIRED_PROPERTIES) {
      if (!trinoProperties.containsKey(requiredProperty)) {
        throw new IllegalArgumentException("Missing required property: " + requiredProperty);
      }
    }

    return trinoProperties;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return ImmutableMap.of();
  }
}
