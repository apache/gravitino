/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import static com.datastrato.graviton.catalog.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.graviton.catalog.PropertyEntry.stringRequiredPropertyEntry;

import com.datastrato.graviton.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  public static final String CATALOG_BACKEND_NAME = "catalog-backend";

  public static final String GRAVITON_JDBC_USER = "jdbc-user";
  public static final String ICEBERG_JDBC_USER = "jdbc.user";

  public static final String GRAVITON_JDBC_PASSWORD = "jdbc-password";
  public static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";
  public static final String ICEBERG_JDBC_INITIALIZE = "jdbc-initialize";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  // Map that maintains the mapping of keys in Graviton to that in Iceberg, for example, users
  // will only need to set the configuration 'catalog-backend' in Graviton and Graviton will change
  // it to `catalogType` automatically and pass it to Iceberg.
  public static final Map<String, String> GRAVITON_CONFIG_TO_ICEBERG =
      ImmutableMap.of(
          GRAVITON_JDBC_USER,
          ICEBERG_JDBC_USER,
          GRAVITON_JDBC_PASSWORD,
          ICEBERG_JDBC_PASSWORD,
          URI,
          URI,
          WAREHOUSE,
          WAREHOUSE);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                CATALOG_BACKEND_NAME,
                "Iceberg catalog type choose properties",
                true,
                IcebergCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(URI, "Iceberg catalog uri config", false, false),
            stringRequiredPropertyEntry(
                WAREHOUSE, "Iceberg catalog warehouse config", false, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitonConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITON_CONFIG_TO_ICEBERG.containsKey(key)) {
            gravitonConfig.put(GRAVITON_CONFIG_TO_ICEBERG.get(key), value);
          }
        });
    return gravitonConfig;
  }
}
