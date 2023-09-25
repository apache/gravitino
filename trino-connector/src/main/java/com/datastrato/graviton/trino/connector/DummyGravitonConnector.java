/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import io.trino.spi.connector.Connector;

/**
 * * DummyGravitonConnector is primarily used to drive the GravitonCatalogManager to load catalog
 * connectors managed in the Graviton server. After users configure the Graviton connector through
 * Trino catalog configuration, a DummyGravitonConnector is initially created. It just a
 * placeholder.
 */
public class DummyGravitonConnector implements Connector {

  public DummyGravitonConnector() {
    super();
  }
}
