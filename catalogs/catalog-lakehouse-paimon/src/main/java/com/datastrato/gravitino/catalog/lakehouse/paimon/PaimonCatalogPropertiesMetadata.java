/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringRequiredPropertyEntry;

import com.datastrato.gravitino.connector.BaseCatalogPropertiesMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
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

  public static final String GRAVITINO_CATALOG_BACKEND = "catalog-backend";
  public static final String GRAVITINO_TABLE_TYPE = "table-type";
  public static final String PAIMON_METASTORE = "metastore";
  public static final String PAIMON_TABLE_TYPE = "table.type";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA;

  public static final Map<String, String> CATALOG_CONFIG_MAPPING =
      ImmutableMap.of(
          GRAVITINO_CATALOG_BACKEND,
          PAIMON_METASTORE,
          GRAVITINO_TABLE_TYPE,
          PAIMON_TABLE_TYPE,
          WAREHOUSE,
          WAREHOUSE,
          URI,
          URI);

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
            stringRequiredPropertyEntry(URI, "Paimon catalog uri config", false, false));
    HashMap<String, PropertyEntry<?>> result = Maps.newHashMap(BASIC_CATALOG_PROPERTY_ENTRIES);
    result.putAll(Maps.uniqueIndex(propertyEntries, PropertyEntry::getName));
    PROPERTIES_METADATA = ImmutableMap.copyOf(result);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
