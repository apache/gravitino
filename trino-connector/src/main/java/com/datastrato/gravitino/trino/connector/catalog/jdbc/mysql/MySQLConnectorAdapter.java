/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import static com.datastrato.gravitino.trino.connector.GravitinoConnectorPluginManager.CONNECTOR_MYSQL;
import static java.util.Collections.emptyList;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Transforming MySQL connector configuration and components into Gravitino connector. */
public class MySQLConnectorAdapter implements CatalogConnectorAdapter {

  private final PropertyConverter catalogConverter;
  private final HasPropertyMeta propertyMetadata;

  public MySQLConnectorAdapter() {
    this.catalogConverter = new JDBCCatalogPropertyConverter();
    this.propertyMetadata = new MySQLPropertyMeta();
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
    return CONNECTOR_MYSQL;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new MySQLMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
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
