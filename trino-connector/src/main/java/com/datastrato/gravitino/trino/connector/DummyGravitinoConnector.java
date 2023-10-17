/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import io.trino.spi.connector.Connector;

/**
 * DummyGravitinoConnector is primarily used to drive the GravitinoCatalogManager to load catalog
 * connectors managed in the Gravitino server. After users configure the Gravitino connector through
 * Trino catalog configuration, a DummyGravitinoFConnector is initially created. It is just a
 * placeholder.
 */
public class DummyGravitinoConnector implements Connector {

  public DummyGravitinoConnector() {
    super();
  }
}
