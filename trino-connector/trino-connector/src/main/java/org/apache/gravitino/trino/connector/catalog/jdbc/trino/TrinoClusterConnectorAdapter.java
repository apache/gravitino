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
package org.apache.gravitino.trino.connector.catalog.jdbc.trino;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;
import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Support trino cluster connector. Transforming cluster connector configuration and components into
 * Gravitino connector.
 */
public class TrinoClusterConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_CLUSTER = "trino";
  /** Configuration key for the Trino cluster connection URL. */
  public static final String TRINO_CLUSTER_URL_KEY = "cloud.trino.connection-url";
  /** Configuration key for the Trino cluster user name. */
  public static final String TRINO_CLUSTER_USER_KEY = "cloud.trino.connection-user";
  /** Configuration key for the Trino cluster password. */
  public static final String TRINO_CLUSTER_PASSWORD_KEY = "cloud.trino.connection-password";
  /** Default user name for Trino cluster connection. */
  public static final String TRINO_CLUSTER_DEFAULT_USER = "admin";

  private final HasPropertyMeta propertyMetadata;

  /**
   * Constructs a new TrinoClusterConnectorAdapter. Initializes the property metadata for Trino
   * cluster-specific configurations.
   */
  public TrinoClusterConnectorAdapter() {
    this.propertyMetadata = new TrinoClusterPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    Map<String, String> config = new HashMap<>();
    String jdbcUrl = catalog.getProperty(TRINO_CLUSTER_URL_KEY, "");
    if (StringUtils.isEmpty(jdbcUrl)) {
      throw new TrinoException(
          GRAVITINO_MISSING_CONFIG, "Missing jdbc url config for the cluster catalog");
    }
    jdbcUrl += "/" + catalog.getName();
    config.put(JDBC_CONNECTION_URL_KEY, jdbcUrl);

    String user = catalog.getProperty(TRINO_CLUSTER_USER_KEY, TRINO_CLUSTER_DEFAULT_USER);
    config.put(JDBC_CONNECTION_USER_KEY, user);

    String password = catalog.getProperty(TRINO_CLUSTER_PASSWORD_KEY, "");
    config.put(JDBC_CONNECTION_PASSWORD_KEY, password);

    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_CLUSTER;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new TrinoClusterMetadataAdapter(
        getTableProperties(), Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }
}
