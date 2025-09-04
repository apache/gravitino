/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.trino.connector.catalog.iceberg;

import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;

/**
 * A property converter for Iceberg catalogs that handles the conversion between Trino and Gravitino
 * property formats. This converter manages various Iceberg-specific configurations including
 * general settings, Hive integration, and S3 storage options.
 */
public class IcebergCatalogPropertyConverter extends CatalogPropertyConverter {

  private static final Set<String> JDBC_BACKEND_REQUIRED_PROPERTIES =
      Set.of("jdbc-driver", "uri", "jdbc-user", "jdbc-password");

  private static final Set<String> HIVE_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  private static final Set<String> REST_BACKEND_REQUIRED_PROPERTIES = Set.of("uri");

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
    Map<String, String> stringStringMap;
    String backend = properties.get("catalog-backend");
    if (backend == null)
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property 'catalog-backend'");
    switch (backend) {
      case "hive":
        stringStringMap = buildHiveBackendProperties(properties);
        break;
      case "jdbc":
        stringStringMap = buildJDBCBackendProperties(properties);
        break;
      case "rest":
        stringStringMap = buildRestBackendProperties(properties);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported backend type: " + backend);
    }
    Map<String, String> config = new HashMap<>();
    // The order of put operations determines the priority of parameters.
    config.putAll(super.gravitinoToEngineProperties(properties));
    config.putAll(stringStringMap);
    return config;
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
    jdbcProperties.put(
        "iceberg.jdbc-catalog.driver-class",
        properties.get(IcebergConstants.GRAVITINO_JDBC_DRIVER));
    jdbcProperties.put("iceberg.jdbc-catalog.connection-url", properties.get(IcebergConstants.URI));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-user",
        properties.get(IcebergConstants.GRAVITINO_JDBC_USER));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.connection-password",
        properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD));
    jdbcProperties.put(
        "iceberg.jdbc-catalog.default-warehouse-dir", properties.get(IcebergConstants.WAREHOUSE));

    jdbcProperties.put(
        "iceberg.jdbc-catalog.catalog-name",
        IcebergPropertiesUtils.getCatalogBackendName(properties));

    return jdbcProperties;
  }

  private Map<String, String> buildRestBackendProperties(Map<String, String> properties) {
    Set<String> missingProperty =
        Sets.difference(REST_BACKEND_REQUIRED_PROPERTIES, properties.keySet());
    if (!missingProperty.isEmpty()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_MISSING_REQUIRED_PROPERTY,
          "Missing required property for Rest backend: " + missingProperty);
    }

    Map<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put("iceberg.catalog.type", "rest");
    jdbcProperties.put("iceberg.rest-catalog.uri", properties.get(IcebergConstants.URI));
    return jdbcProperties;
  }
}
