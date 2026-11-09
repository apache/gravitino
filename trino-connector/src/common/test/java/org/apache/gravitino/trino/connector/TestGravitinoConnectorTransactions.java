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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

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

  private GravitinoConnector connector(Connector delegate) {
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
    return new GravitinoConnector(context);
  }
}
