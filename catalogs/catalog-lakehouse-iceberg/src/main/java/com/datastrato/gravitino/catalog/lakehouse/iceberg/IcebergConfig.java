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
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;
import java.util.Optional;

public class IcebergConfig extends Config {

  public static final ConfigEntry<String> CATALOG_BACKEND =
      new ConfigBuilder(CATALOG_BACKEND_NAME)
          .doc("Catalog backend of Gravitino Iceberg catalog")
          .version("0.2.0")
          .stringConf()
          .createWithDefault("memory");

  public static final ConfigEntry<String> CATALOG_WAREHOUSE =
      new ConfigBuilder(WAREHOUSE)
          .doc("Warehouse directory of catalog")
          .version("0.2.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> CATALOG_URI =
      new ConfigBuilder(URI)
          .doc("The uri config of the Iceberg catalog")
          .version("0.2.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> JDBC_USER =
      new ConfigBuilder(ICEBERG_JDBC_USER)
          .doc("The username of the Jdbc connection")
          .version("0.2.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> JDBC_PASSWORD =
      new ConfigBuilder(ICEBERG_JDBC_PASSWORD)
          .doc("The password of the Jdbc connection")
          .version("0.2.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<Optional<String>> JDBC_DRIVER =
      new ConfigBuilder(GRAVITINO_JDBC_DRIVER)
          .doc("The driver of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithOptional();

  public static final ConfigEntry<Boolean> JDBC_INIT_TABLES =
      new ConfigBuilder(ICEBERG_JDBC_INITIALIZE)
          .doc("Whether to initialize meta tables when create Jdbc catalog")
          .version("0.2.0")
          .booleanConf()
          .createWithDefault(true);

  public Optional<String> getJdbcDriverOptional() {
    return get(JDBC_DRIVER);
  }

  public IcebergConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public IcebergConfig() {
    super(false);
  }
}
