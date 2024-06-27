/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link PropertiesMetadata} that represents Paimon catalog properties metadata.
 */
public class PaimonCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  @VisibleForTesting public static final String GRAVITINO_CATALOG_BACKEND = "catalog-backend";
  @VisibleForTesting public static final String PAIMON_METASTORE = "metastore";
  @VisibleForTesting public static final String WAREHOUSE = "warehouse";
  @VisibleForTesting public static final String URI = "uri";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;
  private static final Map<String, String> GRAVITINO_CONFIG_TO_PAIMON =
      ImmutableMap.of(GRAVITINO_CATALOG_BACKEND, PAIMON_METASTORE, WAREHOUSE, WAREHOUSE, URI, URI);

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            enumImmutablePropertyEntry(
                GRAVITINO_CATALOG_BACKEND,
                "Paimon catalog backend type",
                true,
                PaimonCatalogBackend.class,
                null,
                false,
                false),
            stringRequiredPropertyEntry(WAREHOUSE, "Paimon catalog warehouse config", false, false),
            stringOptionalPropertyEntry(URI, "Paimon catalog uri config", false, null, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  protected Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> gravitinoConfig = Maps.newHashMap();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_PAIMON.containsKey(key)) {
            gravitinoConfig.put(GRAVITINO_CONFIG_TO_PAIMON.get(key), value);
          }
        });
    return gravitinoConfig;
  }
}
