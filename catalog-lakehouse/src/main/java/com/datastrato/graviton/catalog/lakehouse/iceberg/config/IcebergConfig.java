/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.config;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public class IcebergConfig extends Config {

  public static final ConfigEntry<String> CATALOG_IMPL =
      new ConfigBuilder("CatalogImpl")
          .doc("Choose the implementation of the iceberg catalog")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("memory");
}
