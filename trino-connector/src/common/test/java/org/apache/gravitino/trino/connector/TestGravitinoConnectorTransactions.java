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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class TestGravitinoConnectorTransactions {
  @Test
  void testRollbackDelegatesToInternalTransaction() {
    Connector delegate = mock(Connector.class);
    GravitinoConnector connector = connector(delegate);
    ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);

    connector.rollback(new GravitinoTransactionHandle(transaction));

    verify(delegate).rollback(transaction);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void testRollbackPropagatesCleanupFailure() {
    Connector delegate = mock(Connector.class);
    GravitinoConnector connector = connector(delegate);
    ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);
    RuntimeException failure = new IllegalStateException("Failed to remove staging table");
    doThrow(failure).when(delegate).rollback(transaction);

    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () -> connector.rollback(new GravitinoTransactionHandle(transaction))));
    verify(delegate).rollback(transaction);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void testCommitDelegatesToInternalTransaction() {
    Connector delegate = mock(Connector.class);
    GravitinoConnector connector = connector(delegate);
    ConnectorTransactionHandle transaction = mock(ConnectorTransactionHandle.class);

    connector.commit(new GravitinoTransactionHandle(transaction));

    verify(delegate).commit(transaction);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void testRollbackDropsCreatedTablesAfterInternalRollback() {
    Connector delegate = mockDelegate();
    GravitinoMetadata metadata = mock(GravitinoMetadata.class);
    GravitinoConnector connector = connector(delegate, metadata);

    connector.rollback(beginWithMetadata(connector));

    InOrder order = inOrder(delegate, metadata);
    order.verify(delegate).rollback(any());
    order.verify(metadata).dropCreatedTables();
  }

  @Test
  void testRollbackDropsCreatedTablesWhenInternalRollbackFails() {
    Connector delegate = mockDelegate();
    doThrow(new IllegalStateException("rollback failed")).when(delegate).rollback(any());
    GravitinoMetadata metadata = mock(GravitinoMetadata.class);
    GravitinoConnector connector = connector(delegate, metadata);
    ConnectorTransactionHandle transaction = beginWithMetadata(connector);

    assertThrows(IllegalStateException.class, () -> connector.rollback(transaction));
    verify(metadata).dropCreatedTables();
  }

  @Test
  void testFailedCommitDropsCreatedTables() {
    Connector delegate = mockDelegate();
    doThrow(new IllegalStateException("commit failed")).when(delegate).commit(any());
    GravitinoMetadata metadata = mock(GravitinoMetadata.class);
    GravitinoConnector connector = connector(delegate, metadata);
    ConnectorTransactionHandle transaction = beginWithMetadata(connector);

    assertThrows(IllegalStateException.class, () -> connector.commit(transaction));
    verify(metadata).dropCreatedTables();
  }

  @Test
  void testCommitAndShutdownKeepCreatedTables() {
    GravitinoMetadata metadata = mock(GravitinoMetadata.class);
    GravitinoConnector connector = connector(mockDelegate(), metadata);

    connector.commit(beginWithMetadata(connector));
    beginWithMetadata(connector);
    connector.shutdown();
    verify(metadata, never()).dropCreatedTables();
  }

  @Test
  void testMetadataOfEndedTransactionIsMarkedEnded() {
    GravitinoMetadata metadata = mock(GravitinoMetadata.class);
    GravitinoConnector connector = connector(mockDelegate(), metadata);
    ConnectorTransactionHandle transaction =
        connector.beginTransaction(IsolationLevel.READ_COMMITTED, false, true);
    connector.rollback(transaction);

    connector.getMetadata(mock(ConnectorSession.class), transaction);
    verify(metadata).dropCreatedTables();
  }

  private static Connector mockDelegate() {
    Connector delegate = mock(Connector.class);
    when(delegate.beginTransaction(any(), anyBoolean(), anyBoolean()))
        .thenAnswer(invocation -> mock(ConnectorTransactionHandle.class));
    when(delegate.getMetadata(any(), any())).thenReturn(mock(ConnectorMetadata.class));
    return delegate;
  }

  private static ConnectorTransactionHandle beginWithMetadata(GravitinoConnector connector) {
    ConnectorTransactionHandle transaction =
        connector.beginTransaction(IsolationLevel.READ_COMMITTED, false, true);
    connector.getMetadata(mock(ConnectorSession.class), transaction);
    return transaction;
  }

  private GravitinoConnector connector(Connector delegate, GravitinoMetadata metadata) {
    return new GravitinoConnector(context(delegate)) {
      @Override
      protected GravitinoMetadata createGravitinoMetadata(
          CatalogConnectorMetadata catalogConnectorMetadata,
          CatalogConnectorMetadataAdapter metadataAdapter,
          ConnectorMetadata internalMetadata) {
        return metadata;
      }
    };
  }

  private GravitinoConnector connector(Connector delegate) {
    return new GravitinoConnector(context(delegate));
  }

  private CatalogConnectorContext context(Connector delegate) {
    GravitinoCatalog catalog = mock(GravitinoCatalog.class);
    when(catalog.geNameIdentifier()).thenReturn(NameIdentifier.of("metalake", "catalog"));
    CatalogConnectorContext context = mock(CatalogConnectorContext.class);
    when(context.getCatalog()).thenReturn(catalog);
    when(context.getInternalConnector()).thenReturn(delegate);
    when(context.getConfig())
        .thenReturn(new GravitinoConfig(ImmutableMap.of("gravitino.metalake", "metalake")));
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog clientCatalog = mock(Catalog.class);
    when(clientCatalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(clientCatalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(metalake.loadCatalog(any())).thenReturn(clientCatalog);
    when(context.getMetalake()).thenReturn(metalake);
    return context;
  }
}
