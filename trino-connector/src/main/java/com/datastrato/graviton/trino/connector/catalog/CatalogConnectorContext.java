/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.GravitonConnector;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/**
 * The CatalogConnector serves as a communication bridge between the Graviton connector and its
 * internal connectors. It manages the lifecycle, configuration, and runtime environment of internal
 * connectors.
 */
public class CatalogConnectorContext {

  private final NameIdentifier catalogName;
  private final GravitonMetaLake metalake;

  // connector communicates with trino
  private final GravitonConnector connector;

  // internal connector communicates with data storage
  private final Connector internalConnector;

  private final CatalogConnectorAdapter adapter;

  public CatalogConnectorContext(
      NameIdentifier catalogName,
      GravitonMetaLake metalake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalogName = catalogName;
    this.metalake = metalake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

    this.connector = new GravitonConnector(catalogName, this);
  }

  public GravitonMetaLake getMetalake() {
    return metalake;
  }

  public GravitonConnector getConnector() {
    return connector;
  }

  public Connector getInternalConnector() {
    return internalConnector;
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  public void close() {
    this.internalConnector.shutdown();
  }
}
