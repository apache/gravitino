/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector;

import com.google.common.base.Preconditions;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;

/**
 * GravitinoConnector serves as the entry point for operations on the connector managed by Trino and
 * Apache Gravitino. It provides a standard entry point for Trino connectors and delegates their
 * operations to internal connectors.
 */
public class GravitinoConnector implements Connector {

  private final NameIdentifier catalogIdentifier;
  private final CatalogConnectorContext catalogConnectorContext;

  /**
   * Constructs a new GravitinoConnector with the specified catalog identifier and catalog connector
   * context.
   *
   * @param catalogIdentifier the catalog identifier
   * @param catalogConnectorContext the catalog connector context
   */
  public GravitinoConnector(
      NameIdentifier catalogIdentifier, CatalogConnectorContext catalogConnectorContext) {
    this.catalogIdentifier = catalogIdentifier;
    this.catalogConnectorContext = catalogConnectorContext;
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(
      IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    Preconditions.checkNotNull(internalConnector, "Internal connector must not be null");

    ConnectorTransactionHandle internalTransactionHandler =
        internalConnector.beginTransaction(isolationLevel, readOnly, autoCommit);
    Preconditions.checkNotNull(internalTransactionHandler, "Transaction handler must not be null");

    return new GravitinoTransactionHandle(internalTransactionHandler);
  }

  @Override
  public ConnectorMetadata getMetadata(
      ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    GravitinoTransactionHandle gravitinoTransactionHandle =
        (GravitinoTransactionHandle) transactionHandle;

    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorMetadata internalMetadata =
        internalConnector.getMetadata(session, gravitinoTransactionHandle.getInternalHandle());
    Preconditions.checkNotNull(internalMetadata);

    GravitinoMetalake metalake = catalogConnectorContext.getMetalake();

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
  public ConnectorRecordSetProvider getRecordSetProvider() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    ConnectorRecordSetProvider internalRecordSetProvider = internalConnector.getRecordSetProvider();
    return new GravitinoRecordSetProvider(internalRecordSetProvider);
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
    internalConnector.commit(gravitinoTransactionHandle.getInternalHandle());
  }

  @Override
  public ConnectorAccessControl getAccessControl() {
    Connector internalConnector = catalogConnectorContext.getInternalConnector();
    return internalConnector.getAccessControl();
  }

  @Override
  public Set<ConnectorCapabilities> getCapabilities() {
    return catalogConnectorContext.getInternalConnector().getCapabilities();
  }
}
