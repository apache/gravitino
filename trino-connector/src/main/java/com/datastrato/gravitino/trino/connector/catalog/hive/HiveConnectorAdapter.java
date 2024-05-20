/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Transforming Hive connector configuration and components into Gravitino connector. */
public class HiveConnectorAdapter implements CatalogConnectorAdapter {

  private static final AtomicInteger VERSION = new AtomicInteger(0);

  private final HasPropertyMeta propertyMetadata;
  private final PropertyConverter catalogConverter;

  public HiveConnectorAdapter() {
    this.propertyMetadata = new HivePropertyMeta();
    this.catalogConverter = new HiveCatalogPropertyConverter();
  }

  @Override
  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(
        "catalogHandle",
        String.format("%s_v%d:normal:default", catalog.getName(), VERSION.getAndIncrement()));
    config.put("connectorName", "hive");

    Map<String, Object> properties = new HashMap<>();
    properties.put("hive.metastore.uri", catalog.getRequiredProperty("metastore.uris"));
    properties.put("hive.security", "allow-all");
    Map<String, String> trinoProperty =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    properties.putAll(trinoProperty);

    config.put("properties", properties);
    return config;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new HiveMetadataAdapter(
        getSchemaProperties(), getTableProperties(), getColumnProperties());
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return Collections.emptyList();
  }
}
