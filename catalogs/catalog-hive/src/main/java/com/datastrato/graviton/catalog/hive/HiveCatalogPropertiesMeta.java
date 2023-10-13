/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.catalog.BaseCatalogPropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HiveCatalogPropertiesMeta extends BaseCatalogPropertiesMetadata {

  public static final String CLIENT_POOL_SIZE = "client.pool-size";
  public static final int DEFAULT_CLIENT_POOL_SIZE = 1;

  public static final String METASTORE_URIS = "metastore.uris";

  private static final Map<String, PropertyEntry<?>> HIVE_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              METASTORE_URIS,
              PropertyEntry.stringPropertyEntry(
                  METASTORE_URIS, "The Hive metastore URIs", true, true, null, false, false))
          .put(
              CLIENT_POOL_SIZE,
              PropertyEntry.integerOptionalPropertyEntry(
                  CLIENT_POOL_SIZE,
                  "The maximum number of Hive clients in the pool for graviton",
                  true,
                  DEFAULT_CLIENT_POOL_SIZE,
                  false))
          .putAll(BASIC_CATALOG_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    // Hive catalog only needs to specify the metastore URIs, maybe we need to add more by
    // referring to the trino catalog.
    // TODO(yuqi), we can add more properties like username for metastore
    //  (kerberos authentication) when we finish refactor properties framework.
    return HIVE_CATALOG_PROPERTY_ENTRIES;
  }
}
