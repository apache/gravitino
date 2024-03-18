/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/** Trino plugin endpoint, using java spi mechanism */
public class GravitinoPlugin implements Plugin {

  // For testing.
  public static CatalogConnectorManager catalogConnectorManager;
  public static GravitinoAdminClient gravitinoClient;

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new GravitinoConnectorFactory());
  }
}
