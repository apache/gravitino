/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;

public class TestGravitinoPlugin extends GravitinoPlugin {
  private TestGravitinoConnectorFactory factory;
  private CatalogConnectorManager catalogConnectorManager;

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    factory = new TestGravitinoConnectorFactory();
    factory.setHook(this::init);
    return ImmutableList.of(factory);
  }

  private void init() {
    catalogConnectorManager = factory.getCatalogConnectorManager();
  }

  public CatalogConnectorManager getCatalogConnectorManager() {
    return catalogConnectorManager;
  }
}
