/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static java.util.Collections.emptyList;

import com.datastrato.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import java.util.HashMap;
import java.util.Map;

/** Transforming MySQL connector configuration and components into Gravitino connector. */
public class MySQLConnectorAdapter implements CatalogConnectorAdapter {
  private final PropertyConverter catalogConverter;

  public MySQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
  }

  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogHandle", catalog.getName() + ":normal:default");
    config.put("connectorName", "mysql");

    Map<String, String> properties =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    config.put("properties", properties);
    return config;
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new MySQLMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }
}
