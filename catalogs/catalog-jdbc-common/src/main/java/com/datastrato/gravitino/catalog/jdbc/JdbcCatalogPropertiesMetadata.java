/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import static com.datastrato.gravitino.catalog.PropertyEntry.integerPropertyEntry;
import static com.datastrato.gravitino.catalog.PropertyEntry.stringImmutablePropertyEntry;

import com.datastrato.gravitino.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class JdbcCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  private static final List<String> JDBC_PROPERTIES =
      ImmutableList.of(
          JdbcConfig.JDBC_URL.getKey(),
          JdbcConfig.USERNAME.getKey(),
          JdbcConfig.PASSWORD.getKey(),
          JdbcConfig.POOL_MIN_SIZE.getKey(),
          JdbcConfig.POOL_MAX_SIZE.getKey());

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringImmutablePropertyEntry(
                JdbcConfig.JDBC_URL.getKey(),
                JdbcConfig.JDBC_URL.getDoc(),
                false,
                null,
                false,
                false),
            stringImmutablePropertyEntry(
                JdbcConfig.USERNAME.getKey(),
                JdbcConfig.USERNAME.getDoc(),
                false,
                null,
                false,
                false),
            stringImmutablePropertyEntry(
                JdbcConfig.PASSWORD.getKey(),
                JdbcConfig.PASSWORD.getDoc(),
                false,
                null,
                false,
                false),
            integerPropertyEntry(
                JdbcConfig.POOL_MIN_SIZE.getKey(),
                JdbcConfig.POOL_MIN_SIZE.getDoc(),
                false,
                true,
                JdbcConfig.POOL_MIN_SIZE.getDefaultValue(),
                true,
                false),
            integerPropertyEntry(
                JdbcConfig.POOL_MAX_SIZE.getKey(),
                JdbcConfig.POOL_MAX_SIZE.getDoc(),
                false,
                true,
                JdbcConfig.POOL_MAX_SIZE.getDefaultValue(),
                true,
                false));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> result = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (JDBC_PROPERTIES.contains(key)) {
            result.put(key, value);
          }
        });
    return result;
  }
}
