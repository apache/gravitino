/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.postgresql;

import static java.util.Collections.emptyList;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import java.util.HashMap;
import java.util.Map;

/** Transforming PostgreSQL connector configuration and components into Gravitino connector. */
public class PostgreSQLConnectorAdapter implements CatalogConnectorAdapter {

  public PostgreSQLConnectorAdapter() {}

  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogHandle", catalog.getName() + ":normal:default");
    config.put("connectorName", "postgresql");

    Map<String, Object> properties = new HashMap<>();
    properties.put("connection-url", catalog.getRequiredProperty("jdbc-url"));
    properties.put("connection-user", catalog.getRequiredProperty("jdbc-user"));
    properties.put("connection-password", catalog.getRequiredProperty("jdbc-password"));
    config.put("properties", properties);
    return config;
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new PostgreSQLMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }
}
