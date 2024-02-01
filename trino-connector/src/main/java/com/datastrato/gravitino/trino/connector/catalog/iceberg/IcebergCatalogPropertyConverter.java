/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.catalog.property.PropertyConverter;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class IcebergCatalogPropertyConverter extends PropertyConverter {

  private static final TreeBidiMap<String, String> TRINO_ICEBERG_TO_GRAVITINO_ICEBERG =
      new TreeBidiMap<>(new ImmutableMap.Builder<String, String>().build());

  private static final Set<String> JDBC_BACKEND_REQUIRED_PROPERTIES =
      Set.of("jdbc-driver", "uri", "jdbc-user", "jdbc-password");

  private static final Set<String> HIVE_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_ICEBERG_TO_GRAVITINO_ICEBERG;
  }

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
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
    Set<String> missingProperty =
        Sets.difference(HIVE_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for Hive backend: " + missingProperty);
    }

    Map<String, String> hiveProperties = new HashMap<>();
    hiveProperties.put("iceberg.catalog.type", "hive_metastore");
    hiveProperties.put("hive.metastore.uri", properties.get("uri"));
    return hiveProperties;
  }

  private Map<String, String> buildJDBCBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(JDBC_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for JDBC backend: " + missingProperty);
    }

    Map<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put("iceberg.catalog.type", "jdbc");
    jdbcProperties.put("iceberg.jdbc-catalog.driver-class", properties.get("jdbc-driver"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get("uri"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-user", properties.get("jdbc-user"));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-password", properties.get("jdbc-password"));
    jdbcProperties.put("iceberg.jdbc-catalog.default-warehouse-dir", properties.get("warehouse"));

    // TODO (FANG) make the catalog name equal to the catalog name in Gravitino
    jdbcProperties.put("iceberg.jdbc-catalog.catalog-name", "jdbc");

    return jdbcProperties;
  }

  @Override
  public Map<String, PropertyEntry<?>> gravitinoPropertyMeta() {
    return ImmutableMap.of();
  }
}
