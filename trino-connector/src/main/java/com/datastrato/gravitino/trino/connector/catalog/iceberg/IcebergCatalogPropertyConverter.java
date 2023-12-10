/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.shaded.org.apache.commons.collections4.bidimap.TreeBidiMap;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.datastrato.gravitino.trino.connector.catalog.PropertyConverter;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class IcebergCatalogPropertyConverter extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITINO_ICEBERG =
      new TreeBidiMap<>(new ImmutableMap.Builder<String, String>().build());

  @Override
  public TreeBidiMap<String, String> trinoPropertyKeyToGravitino() {
    return TRINO_ICEBERG_TO_GRAVITINO_ICEBERG;
  }

  @Override
  public Map<String, String> toTrinoProperties(Map<String, String> properties) {
    String backend = properties.get("catalog-backend");
    switch (backend) {
      case "hive":
        return buildHiveBackendProperties(properties);
      case "jdbc":
        return buildJDBCBackendProperties(properties);
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backend);
    }
  }

  private Map<String, String> buildHiveBackendProperties(Map<String, String> properties) {
    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("iceberg.catalog.type", "hive_metastore");
    hiveProperties.put("hive.metastore.uri", properties.get("uri"));
    return hiveProperties;
  }

  private Map<String, String> buildJDBCBackendProperties(Map<String, String> properties) {
    Map<String, String> jdbcProperties = new HashMap<>();
    Driver driver;
    try {
      driver = DriverManager.getDriver(properties.get("uri"));
    } catch (SQLException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ICEBERG_UNSUPPORTED_JDBC_TYPE,
          "Invalid JDBC URI: " + properties.get("uri"),
          e);
    }

    String driverClass = driver.getClass().getCanonicalName();

    jdbcProperties.put("iceberg.catalog.type", "jdbc");
    jdbcProperties.put("iceberg.jdbc-catalog.driver-class", driverClass);
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get("uri"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-user", properties.get("jdbc-user"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-password", properties.get("jdbc-password"));
    jdbcProperties.put("iceberg.jdbc-catalog.default-warehouse-dir", properties.get("warehouse"));

    // TODO (FANG) make the catalog name equal to the catalog name in Gravitino
    jdbcProperties.put("iceberg.jdbc-catalog.catalog-name", "jdbc");

    return jdbcProperties;
  }
}
