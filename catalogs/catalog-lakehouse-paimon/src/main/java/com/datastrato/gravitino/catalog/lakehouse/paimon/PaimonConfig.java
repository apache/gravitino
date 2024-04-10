/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.options.CatalogOptions;

public class PaimonConfig extends Config {

  public static final ConfigEntry<String> METASTORE =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.PAIMON_METASTORE)
          .doc(CatalogOptions.METASTORE.description().toString())
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault(CatalogOptions.METASTORE.defaultValue());

  public static final ConfigEntry<String> WAREHOUSE =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.WAREHOUSE)
          .doc(CatalogOptions.WAREHOUSE.description().toString())
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> URI =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.URI)
          .doc(CatalogOptions.URI.description().toString())
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> TABLE_TYPE =
      new ConfigBuilder(PaimonCatalogPropertiesMetadata.PAIMON_TABLE_TYPE)
          .doc(CatalogOptions.TABLE_TYPE.description().toString())
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault(CatalogOptions.TABLE_TYPE.defaultValue().toString());

  public PaimonConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public PaimonConfig() {
    super(false);
  }

  public static PaimonConfig loadPaimonConfig(Map<String, String> properties) {
    return new PaimonConfig(
        properties.entrySet().stream()
            .filter(
                property ->
                    PaimonCatalogPropertiesMetadata.CATALOG_CONFIG_MAPPING.containsKey(
                        property.getKey()))
            .collect(
                Collectors.toMap(
                    property ->
                        PaimonCatalogPropertiesMetadata.CATALOG_CONFIG_MAPPING.get(
                            property.getKey()),
                    Map.Entry::getValue)));
  }
}
