/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import static com.datastrato.gravitino.trino.connector.GravitinoConnectorPluginManager.CONNECTOR_POSTGRESQL;
import static java.util.Collections.emptyList;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import java.util.HashMap;
import java.util.Map;

/** Transforming PostgreSQL connector configuration and components into Gravitino connector. */
public class PostgreSQLConnectorAdapter implements CatalogConnectorAdapter {
  private final PropertyConverter catalogConverter;

  public PostgreSQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config = new HashMap<>();
    Map<String, String> properties =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    config.putAll(properties);
    return config;
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
