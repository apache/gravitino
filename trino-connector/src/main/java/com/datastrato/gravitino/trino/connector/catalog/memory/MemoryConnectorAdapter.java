/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog.memory;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Support trino Memory connector for testing. Transforming Memory connector configuration and
 * components into Gravitino connector.
 */
public class MemoryConnectorAdapter implements CatalogConnectorAdapter {

  @Override
  public Map<String, Object> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    Map<String, Object> config = new HashMap<>();
    config.put("catalogHandle", catalog.getName() + ":normal:default");
    config.put("connectorName", "memory");

    Map<String, Object> properties = new HashMap<>();
    config.put("properties", properties);
    return config;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new MemoryMetadataAdapter(
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }
}
