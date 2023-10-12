/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import static com.datastrato.graviton.Catalog.PROPERTY_PACKAGE;
import static com.datastrato.graviton.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.graviton.catalog.BasePropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveCatalogPropertiesMeta extends BasePropertiesMetadata {

  public static final String CATALOG_CLIENT_POOL_MAXSIZE = "hive.client.pool.max-size";
  public static final int DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE = 1;

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    // Hive catalog only needs to specify the metastore URIs, maybe we need to add more by
    // referring to the trino catalog.
    // TODO(yuqi), we can add more properties like username for metastore
    //  (kerberos authentication) when we finish refactor properties framework.
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .put(
            CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.METASTOREURIS.varname,
            PropertyEntry.stringPropertyEntry(
                CATALOG_BYPASS_PREFIX + HiveConf.ConfVars.METASTOREURIS.varname,
                "The Hive metastore URIs",
                true,
                true,
                null,
                false,
                false))
        .put(
            CATALOG_CLIENT_POOL_MAXSIZE,
            PropertyEntry.integerOptionalPropertyEntry(
                CATALOG_CLIENT_POOL_MAXSIZE,
                "The maximum number of Hive clients in the pool for graviton",
                true,
                DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE,
                false))
        .put(
            PROPERTY_PACKAGE,
            PropertyEntry.stringPropertyEntry(
                PROPERTY_PACKAGE,
                "The path of the catalog-related classes and resources",
                false,
                true,
                null,
                false,
                false))
        .build();
  }
}
