/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import static java.util.Collections.emptyList;

import com.datastrato.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Transforming PostgreSQL connector configuration and components into Gravitino connector. */
public class PostgreSQLConnectorAdapter implements CatalogConnectorAdapter {
  private final PropertyConverter catalogConverter;
  private static final AtomicInteger VERSION = new AtomicInteger(0);

  public PostgreSQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
  }

  @Override
  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(
        "catalogHandle",
        String.format("%s_v%d:normal:default", catalog.getName(), VERSION.getAndIncrement()));
    config.put("connectorName", "postgresql");

    Map<String, String> properties =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    config.put("properties", properties);
    return config;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new PostgreSQLMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }
}
