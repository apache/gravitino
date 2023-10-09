/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class IcebergConfig extends Config {

  public static final ConfigEntry<String> CATALOG_TYPE =
      new ConfigBuilder("catalogType")
          .doc("Choose the implementation of the Iceberg catalog")
          .version(DEFAULT_VERSION)
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<Boolean> INITIALIZE_JDBC_CATALOG_TABLES =
      new ConfigBuilder("initializeJdbcCatalogTables")
          .doc("Whether to load the configuration of the jdbc catalog table during initialization")
          .version(DEFAULT_VERSION)
          .booleanConf()
          .createWithDefault(true);

  public IcebergConfig() {
    super(false);
  }
}
