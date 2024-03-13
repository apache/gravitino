/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;

public class TestGravitinoPlugin extends GravitinoPlugin {
  private GravitinoConnectorFactory factory;

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    factory = new GravitinoConnectorFactory();
    return ImmutableList.of(factory);
  }

  public CatalogConnectorManager getCatalogConnectorManager() {
    return factory.getCatalogConnectorManager();
  }
}
