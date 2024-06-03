/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.trino.connector.GravitinoConnector;
import com.datastrato.gravitino.trino.connector.GravitinoConnectorPluginManager;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

/**
 * The CatalogConnector serves as a communication bridge between the Gravitino connector and its
 * internal connectors. It manages the lifecycle, configuration, and runtime environment of internal
 * connectors.
 */
public class CatalogConnectorContext {

  private final GravitinoCatalog catalog;
  private final GravitinoMetalake metalake;

  // Connector communicates with trino
  private final GravitinoConnector connector;

  // Internal connector communicates with data storage
  private final Connector internalConnector;

  private final CatalogConnectorAdapter adapter;

  public CatalogConnectorContext(
      GravitinoCatalog catalog,
      GravitinoMetalake metalake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalog = catalog;
    this.metalake = metalake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

    this.connector = new GravitinoConnector(catalog.geNameIdentifier(), this);
  }

  public GravitinoMetalake getMetalake() {
    return metalake;
  }

  public GravitinoCatalog getCatalog() {
    return catalog;
  }

  public GravitinoConnector getConnector() {
    return connector;
  }

  public Connector getInternalConnector() {
    return internalConnector;
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return adapter.getSchemaProperties();
  }

  public List<PropertyMetadata<?>> getColumnProperties() {
    return adapter.getColumnProperties();
  }

  public void close() {
    this.internalConnector.shutdown();
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return adapter.getMetadataAdapter();
  }

  static class Builder {
    private final CatalogConnectorAdapter connectorAdapter;
    private GravitinoCatalog catalog;
    private GravitinoMetalake metalake;
    private ConnectorContext context;

    Builder(CatalogConnectorAdapter connectorAdapter) {
      this.connectorAdapter = connectorAdapter;
    }

    private Builder(CatalogConnectorAdapter connectorAdapter, GravitinoCatalog catalog) {
      this.connectorAdapter = connectorAdapter;
      this.catalog = catalog;
    }

    public Builder clone(GravitinoCatalog catalog) {
      return new Builder(connectorAdapter, catalog);
    }

    Builder withMetalake(GravitinoMetalake metalake) {
      this.metalake = metalake;
      return this;
    }

    Builder withContext(ConnectorContext context) {
      this.context = context;
      return this;
    }

    CatalogConnectorContext build() throws Exception {
      Preconditions.checkArgument(metalake != null, "metalake is not null");
      Preconditions.checkArgument(catalog != null, "catalog is not null");
      Preconditions.checkArgument(context != null, "context is not null");
      Map<String, String> connectorConfig = connectorAdapter.buildInternalConnectorConfig(catalog);
      String internalConnectorName = connectorAdapter.internalConnectorName();

      Connector connector =
          GravitinoConnectorPluginManager.instance(context.getClass().getClassLoader())
              .createConnector(internalConnectorName, connectorConfig, context);
      return new CatalogConnectorContext(catalog, metalake, connector, connectorAdapter);
    }
  }
}
