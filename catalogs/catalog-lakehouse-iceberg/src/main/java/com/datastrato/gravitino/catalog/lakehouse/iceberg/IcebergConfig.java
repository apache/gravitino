/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.CATALOG_BACKEND_NAME;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.GRAVITINO_JDBC_DRIVER;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_INITIALIZE;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_PASSWORD;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.ICEBERG_JDBC_USER;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.URI;
import static com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergCatalogPropertiesMetadata.WAREHOUSE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics.IcebergMetricsManager;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.datastrato.gravitino.server.web.OverwriteDefaultConfig;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class IcebergConfig extends Config implements OverwriteDefaultConfig {

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(CATALOG_BACKEND_NAME)
          .doc("Catalog backend of Gravitino Iceberg catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(WAREHOUSE)
          .doc("Warehouse directory of catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(URI)
          .doc("The uri config of the Iceberg catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_USER =
      new ConfigBuilder(ICEBERG_JDBC_USER)
          .doc("The username of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_PASSWORD =
      new ConfigBuilder(ICEBERG_JDBC_PASSWORD)
          .doc("The password of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_DRIVER =
      new ConfigBuilder(GRAVITINO_JDBC_DRIVER)
          .doc("The driver of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Boolean> JDBC_INIT_TABLES =
      new ConfigBuilder(ICEBERG_JDBC_INITIALIZE)
          .doc("Whether to initialize meta tables when create Jdbc catalog")
          .version(ConfigConstants.VERSION_0_2_0)
          .booleanConf()
          .createWithDefault(true);

  public static final ConfigEntry<String> ICEBERG_METRICS_STORE =
      new ConfigBuilder(IcebergMetricsManager.ICEBERG_METRICS_STORE)
          .doc("The store to save Iceberg metrics")
          .version(ConfigConstants.VERSION_0_4_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Integer> ICEBERG_METRICS_STORE_RETAIN_DAYS =
      new ConfigBuilder(IcebergMetricsManager.ICEBERG_METRICS_STORE_RETAIN_DAYS)
          .doc(
              "The retain days of Iceberg metrics, the value not greater than 0 means retain forever")
          .version(ConfigConstants.VERSION_0_4_0)
          .intConf()
          .createWithDefault(-1);

  public static final ConfigEntry<Integer> ICEBERG_METRICS_QUEUE_CAPACITY =
      new ConfigBuilder(IcebergMetricsManager.ICEBERG_METRICS_QUEUE_CAPACITY)
          .doc("The capacity for Iceberg metrics queues, should greater than 0")
          .version(ConfigConstants.VERSION_0_4_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(1000);

  public String getJdbcDriver() {
    return get(JDBC_DRIVER);
  }

  public IcebergConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public IcebergConfig() {
    super(false);
  }

  @Override
  public Map<String, String> getOverwriteDefaultConfig() {
    return ImmutableMap.of(
        JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
        String.valueOf(JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT),
        JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
        String.valueOf(JettyServerConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT));
  }
}
