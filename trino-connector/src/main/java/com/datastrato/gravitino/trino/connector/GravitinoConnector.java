/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import com.google.common.base.Preconditions;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;

/**
 * GravitinoConnector serves as the entry point for operations on the connector managed by Trino and
 * Gravitino. It provides a standard entry point for Trino connectors and delegates their operations
 * to internal connectors.
 */
public class GravitinoConnector implements Connector {

  private final NameIdentifier catalogIdentifier;
  private final CatalogConnectorContext catalogConnectorContext;

  public GravitinoConnector(
      NameIdentifier catalogIdentifier, CatalogConnectorContext catalogConnectorContext) {
    this.catalogIdentifier = catalogIdentifier;
    this.catalogConnectorContext = catalogConnectorContext;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();

    ConnectorTransactionHandle internalTransactionHandler =
        internalConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    Preconditions.checkNotNull(internalConnector);

    return new GravitinoTransactionHandle(internalTransactionHandler);
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;

    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorMetadata internalMetadata =
        internalConnector.getMetadata(
            session, gravitinoTransactionHandle.getInternalTransactionHandle());
    Preconditions.checkNotNull(internalMetadata);

    GravitinoMetaLake metalake = catalogConnectorContext.getMetalake();

    CatalogConnectorMetadata catalogConnectorMetadata =
        new CatalogConnectorMetadata(metalake, catalogIdentifier);

    return new GravitinoMetadata(
        catalogConnectorMetadata, catalogConnectorContext.getMetadataAdapter(), internalMetadata);
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return catalogConnectorContext.getTableProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getSessionProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return catalogConnectorContext.getColumnProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return catalogConnectorContext.getSchemaProperties();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorSplitManager splitManager = internalConnector.getSplitManager();
    return new GravitinoSplitManager(splitManager);
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorPageSourceProvider internalPageSourceProvider =
        internalConnector.getPageSourceProvider();
    return new GravitinoDataSourceProvider(internalPageSourceProvider);
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorPageSinkProvider pageSinkProvider = internalConnector.getPageSinkProvider();
    return new GravitinoPageSinkProvider(pageSinkProvider);
  }

  @Override
  public void commit(ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    internalConnector.commit(gravitinoTransactionHandle.getInternalTransactionHandle());
  }

  @Override
  public ConnectorAccessControl getAccessControl() {
    return new AllowAllAccessControl();
  }
}
