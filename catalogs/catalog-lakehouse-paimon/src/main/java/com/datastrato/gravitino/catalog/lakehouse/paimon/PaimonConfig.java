/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.options.CatalogOptions;

public class PaimonConfig extends Config {

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE)
          .doc(CatalogOptions.METASTORE.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault(CatalogOptions.METASTORE.defaultValue());

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.WAREHOUSE)
          .doc(CatalogOptions.WAREHOUSE.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.URI)
          .doc(CatalogOptions.URI.description().toString())
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .create();

  public PaimonConfig() {
    super(false);
  }

  public PaimonConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
