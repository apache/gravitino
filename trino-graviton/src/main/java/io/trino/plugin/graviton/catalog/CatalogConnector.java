/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import io.trino.plugin.graviton.GravitonConnector;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/**
 * The CatalogConnector serves as a communication bridge between the Graviton connector and its
 * internal connectors. It manages the lifecycle, configuration, and runtime environment of internal
 * connectors.
 */
public class CatalogConnector {

  protected final NameIdentifier catalogName;
  protected final GravitonMetaLake metaLake;

  protected GravitonConnector connector;
  protected Connector internalConnector;

  private CatalogConnectorAdapter adapter;

  public CatalogConnector(
      NameIdentifier catalogName,
      GravitonMetaLake metaLake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalogName = catalogName;
    this.metaLake = metaLake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

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
}
