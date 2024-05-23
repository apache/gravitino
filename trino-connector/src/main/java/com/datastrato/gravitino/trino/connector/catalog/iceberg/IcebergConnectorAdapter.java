/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import static java.util.Collections.emptyList;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import org.apache.commons.lang3.NotImplementedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Transforming Iceberg connector configuration and components into Gravitino connector. */
public class IcebergConnectorAdapter implements CatalogConnectorAdapter {

  private final IcebergPropertyMeta propertyMetadata;
  private final PropertyConverter catalogConverter;

  public IcebergConnectorAdapter() {
    this.propertyMetadata = new IcebergPropertyMeta();
    this.catalogConverter = new IcebergCatalogPropertyConverter();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("connector.name", "iceberg");

    Map<String, String> properties =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    config.putAll(properties);
    return config;
  }

  @Override
  public Connector buildInternalConnector(Map<String, String> config) throws Exception {
    throw new NotImplementedException();
  }

  @Override
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
