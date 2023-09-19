/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton;

import com.datastrato.graviton.NameIdentifier;
import io.trino.plugin.graviton.catalog.CatalogConnector;
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

public class GravitonConnector implements Connector {

  private final NameIdentifier catalogIdentifier;
  private final CatalogConnector catalogConnector;
  private final Connector internalConnector;

  public GravitonConnector(NameIdentifier catalogIdentifier, CatalogConnector catalogConnector) {
    this.catalogIdentifier = catalogIdentifier;
    this.catalogConnector = catalogConnector;
    this.internalConnector = catalogConnector.getInternalConnector();
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    throw new NotImplementedException();
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    throw new NotImplementedException();
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return catalogConnector.getTableProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSessionProperties() {
    return internalConnector.getSessionProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return internalConnector.getColumnProperties();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return internalConnector.getSchemaProperties();
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    throw new NotImplementedException();
  }

  @Override
  public final void shutdown() {
    internalConnector.shutdown();
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
    throw new NotImplementedException();
  }
}
