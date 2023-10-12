/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorContext;
import com.datastrato.graviton.trino.connector.catalog.CatalogConnectorMetadata;
import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

/**
 * GravitonConnector serves as the entry point for operations on the connector managed by Trino and
 * Graviton. It provides a standard entry point for Trino connectors and delegates their operations
 * to internal connectors.
 */
public class GravitonConnector implements Connector {

  private final NameIdentifier catalogIdentifier;
  private final CatalogConnectorContext catalogConnectorContext;

  public GravitonConnector(
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

    return new GravitonTransactionHandle(internalTransactionHandler);
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    GravitonTransactionHandle gravitonTransactionHandle =
        (GravitonTransactionHandle) transactionHandle;

    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorMetadata internalMetadata =
        internalConnector.getMetadata(
            session, gravitonTransactionHandle.getInternalTransactionHandle());
    Preconditions.checkNotNull(internalMetadata);

    GravitonMetaLake metalake = catalogConnectorContext.getMetalake();

    CatalogConnectorMetadata catalogConnectorMetadata =
        new CatalogConnectorMetadata(metalake, catalogIdentifier);

    return new GravitonMetadata(
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
    throw new NotImplementedException();
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    throw new NotImplementedException();
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    throw new NotImplementedException();
  }

  @Override
  public void commit(ConnectorTransactionHandle transactionHandle) {
    GravitonTransactionHandle gravitonTransactionHandle =
        (GravitonTransactionHandle) transactionHandle;
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    internalConnector.commit(gravitonTransactionHandle.getInternalTransactionHandle());
  }
}
