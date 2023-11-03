/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc;

import static com.datastrato.gravitino.catalog.PropertyEntry.integerPropertyEntry;
import static com.datastrato.gravitino.catalog.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class JdbcCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(
                JdbcConfig.JDBC_URL.getKey(), JdbcConfig.JDBC_URL.getDoc(), true),
            stringReservedPropertyEntry(
                JdbcConfig.USERNAME.getKey(), JdbcConfig.USERNAME.getDoc(), true),
            stringReservedPropertyEntry(
                JdbcConfig.PASSWORD.getKey(), JdbcConfig.PASSWORD.getDoc(), true),
            integerPropertyEntry(
                JdbcConfig.POOL_MIN_IDLE.getKey(),
                JdbcConfig.POOL_MIN_IDLE.getDoc(),
                false,
                false,
                JdbcConfig.POOL_MIN_IDLE.getDefaultValue(),
                true,
                true),
            integerPropertyEntry(
                JdbcConfig.POOL_MAX_SIZE.getKey(),
                JdbcConfig.POOL_MAX_SIZE.getDoc(),
                false,
                false,
                JdbcConfig.POOL_MAX_SIZE.getDefaultValue(),
                true,
                true));
    PROPERTIES_METADATA = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
