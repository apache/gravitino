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
package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/** Transforming MySQL connector configuration and components into Apache Gravitino connector. */
public class MySQLConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_MYSQL = "mysql";
  private final PropertyConverter catalogConverter;
  private final HasPropertyMeta propertyMetadata;

  /**
   * Constructs a new MySQLConnectorAdapter. Initializes the catalog property converter and property
   * metadata for MySQL-specific configurations.
   */
  public MySQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
    this.propertyMetadata = new MySQLPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    return catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_MYSQL;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new MySQLMetadataAdapter(
        getSchemaProperties(), getTableProperties(), getColumnProperties());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return propertyMetadata.getColumnPropertyMetadata();
  }
}
