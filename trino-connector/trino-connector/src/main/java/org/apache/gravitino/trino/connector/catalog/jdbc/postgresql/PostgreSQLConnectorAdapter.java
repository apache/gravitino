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
package org.apache.gravitino.trino.connector.catalog.jdbc.postgresql;

import static java.util.Collections.emptyList;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Transforming PostgreSQL connector configuration and components into Apache Gravitino connector.
 */
public class PostgreSQLConnectorAdapter implements CatalogConnectorAdapter {
  private static final String CONNECTOR_POSTGRESQL = "postgresql";
  private final PropertyConverter catalogConverter;

  /**
   * Constructs a new PostgreSQLConnectorAdapter. Initializes the catalog property converter for
   * PostgreSQL-specific configurations.
   */
  public PostgreSQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> trinoProperty =
        new HashMap<>(catalogConverter.gravitinoToEngineProperties(catalog.getProperties()));
    trinoProperty.put("postgresql.array-mapping", "AS_ARRAY");
    return trinoProperty;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_POSTGRESQL;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new PostgreSQLMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }
}
