/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.GravitonConnector;
import com.google.common.base.Preconditions;
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
  private final GravitonMetaLake metaLake;

  // connector communicate with trino
  protected GravitonConnector connector;
  // inter connector communicate with data storage
  protected Connector internalConnector;

  private CatalogConnectorAdapter adapter;

  public CatalogConnectorContext(
      NameIdentifier catalogName,
      GravitonMetaLake metaLake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalogName = Preconditions.checkNotNull(catalogName, "catalogName is not null");
    this.metaLake = Preconditions.checkNotNull(metaLake, "metaLake is not null");
    this.internalConnector =
        Preconditions.checkNotNull(internalConnector, "internalConnector is not null");
    this.adapter = Preconditions.checkNotNull(adapter, "adapter is not null");

    this.connector = new GravitonConnector(catalogName, this);
  }

  public GravitonMetaLake getMetaLake() {
    return metaLake;
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
