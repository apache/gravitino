/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;

public class TestGravitinoPlugin extends GravitinoPlugin {
  private TestGravitinoConnectorFactory factory;

  private final GravitinoAdminClient gravitinoClient;

  public TestGravitinoPlugin(GravitinoAdminClient gravitinoClient) {
    this.gravitinoClient = gravitinoClient;
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    factory = new TestGravitinoConnectorFactory();
    factory.setGravitinoClient(gravitinoClient);
    return ImmutableList.of(factory);
  }

  public CatalogConnectorManager getCatalogConnectorManager() {
    return factory.getCatalogConnectorManager();
  }
}
