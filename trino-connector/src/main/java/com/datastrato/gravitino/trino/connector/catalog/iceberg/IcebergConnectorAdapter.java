/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import static java.util.Collections.emptyList;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Transforming Iceberg connector configuration and components into Gravitino connector. */
public class IcebergConnectorAdapter implements CatalogConnectorAdapter {

  private final IcebergPropertyMeta propertyMetadata;

  public IcebergConnectorAdapter() {
    this.propertyMetadata = new IcebergPropertyMeta();
  }

  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogHandle", catalog.getName() + ":normal:default");
    config.put("connectorName", "iceberg");

    Map<String, Object> properties = new HashMap<>();
    properties.put("iceberg.catalog.type", "hive_metastore");
    properties.put("hive.metastore.uri", catalog.getProperties("uri", ""));
    config.put("properties", properties);
    return config;
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new IcebergMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }
}
