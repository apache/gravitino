/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.catalog.BasePropertiesMetadata;
import com.datastrato.graviton.catalog.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveCatalogPropertiesMeta extends BasePropertiesMetadata {

  public static final String CATALOG_CLIENT_POOL_MAXSIZE = "graviton.hive.client.pool.max-size";
  public static final int DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE = 1;

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    // Hive catalog only needs to specify the metastore URIs, maybe we need to add more by
    // referring to the trino catalog.
    // TODO(yuqi), we can add more properties like username for metastore
    //  (kerberos authentication) when we finish refactor properties framework.
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .put(
            HiveConf.ConfVars.METASTOREURIS.varname,
            PropertyEntry.stringPropertyEntry(
                HiveConf.ConfVars.METASTOREURIS.varname,
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
                "The maximum number of clients in the pool",
                true,
                DEFAULT_CATALOG_CLIENT_POOL_MAXSIZE,
                false))
        .build();
  }
}
