/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog.hive;

import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.graviton.trino.connector.metadata.GravitonCatalog;
import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Transforming Hive connector configuration and components into Graviton connector. */
public class HiveConnectorAdapter implements CatalogConnectorAdapter {

  private final HiveTableProperties tableProperties = new HiveTableProperties();

  public HiveConnectorAdapter() {}

  public Map<String, Object> buildInternalConnectorConfig(GravitonCatalog catalog) {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogHandle", catalog.getName() + ":normal:default");
    config.put("connectorName", "hive");

    Map<String, Object> properties = new HashMap<>();
    properties.put("hive.metastore.uri", catalog.getProperties("hive.metastore.uris", ""));
    config.put("properties", properties);
    return config;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return tableProperties.getTableProperties();
  }
}
