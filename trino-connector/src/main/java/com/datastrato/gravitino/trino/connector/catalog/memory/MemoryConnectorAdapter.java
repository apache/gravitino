/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.memory;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Support trino Memory connector for testing. Transforming Memory connector configuration and
 * components into Gravitino connector.
 */
public class MemoryConnectorAdapter implements CatalogConnectorAdapter {

  private final HasPropertyMeta propertyMetadata;

  public MemoryConnectorAdapter() {
    this.propertyMetadata = new MemoryPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    Map<String, String> config = new HashMap<>();
    config.put("connector.name", "memory");
    return config;
  }

  @Override
  public Connector buildInternalConnector(Map<String, String> config) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new MemoryMetadataAdapter(
        getTableProperties(), Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }
}
