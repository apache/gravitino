/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.catalog.PropertyEntry.stringRequiredPropertyEntry;

import com.datastrato.gravitino.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalogs.Iceberg.IcebergCatalogPropertyKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {
  public static final String ICEBERG_JDBC_USER = "jdbc.user";

  public static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";
  public static final String ICEBERG_JDBC_INITIALIZE = "jdbc-initialize";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  // Map that maintains the mapping of keys in Gravitino to that in Iceberg, for example, users
  // will only need to set the configuration 'catalog-backend' in Gravitino and Gravitino will
  // change
  // it to `catalogType` automatically and pass it to Iceberg.
  public static final Map<String, String> GRAVITINO_CONFIG_TO_ICEBERG =
      ImmutableMap.of(
          IcebergCatalogPropertyKeys.CATALOG_BACKEND_NAME,
          IcebergCatalogPropertyKeys.CATALOG_BACKEND_NAME,
          IcebergCatalogPropertyKeys.JDBC_BACKEND_CONNECTION_DRIVER_KEY,
          IcebergCatalogPropertyKeys.JDBC_BACKEND_CONNECTION_DRIVER_KEY,
          IcebergCatalogPropertyKeys.JDBC_BACKEND_CONNECTION_USER_KEY,
          ICEBERG_JDBC_USER,
          IcebergCatalogPropertyKeys.JDBC_BACKEND_CONNECTION_PASSWORD_KEY,
          ICEBERG_JDBC_PASSWORD,
          IcebergCatalogPropertyKeys.URI,
          IcebergCatalogPropertyKeys.URI,
          IcebergCatalogPropertyKeys.WAREHOUSE,
          IcebergCatalogPropertyKeys.WAREHOUSE);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                IcebergCatalogPropertyKeys.CATALOG_BACKEND_NAME,
                "Iceberg catalog type choose properties",
                true,
                IcebergCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(
                IcebergCatalogPropertyKeys.URI, "Iceberg catalog uri config", false, false),
            stringRequiredPropertyEntry(
                IcebergCatalogPropertyKeys.WAREHOUSE,
                "Iceberg catalog warehouse config",
                false,
                false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitinoConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_ICEBERG.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_ICEBERG.get(key), value);
          }
        });
    return gravitinoConfig;
  }
}
