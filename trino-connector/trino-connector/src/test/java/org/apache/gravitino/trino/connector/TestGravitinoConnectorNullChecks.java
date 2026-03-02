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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.Connector;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Test;

class TestGravitinoConnectorNullChecks {
  @Test
  void testBeginTransactionThrowsIfInternalConnectorIsNull() {
    GravitinoCatalog mockCatalog = mock(GravitinoCatalog.class);
    when(mockCatalog.geNameIdentifier()).thenReturn(NameIdentifier.of("metalake", "catalog"));

    CatalogConnectorContext mockContext = mock(CatalogConnectorContext.class);
    when(mockContext.getCatalog()).thenReturn(mockCatalog);
    GravitinoMetalake metalake = mockMetalake();
    when(mockContext.getMetalake()).thenReturn(metalake);
    when(mockContext.getInternalConnector()).thenReturn(null);
    GravitinoConnector connector = new GravitinoConnector(mockContext);
    assertThrows(
        IllegalArgumentException.class,
        () -> connector.beginTransaction(IsolationLevel.READ_COMMITTED, true, true));
  }

  @Test
  void testBeginTransactionThrowsIfInternalTransactionHandleIsNull() {
    GravitinoCatalog mockCatalog = mock(GravitinoCatalog.class);
    when(mockCatalog.geNameIdentifier()).thenReturn(NameIdentifier.of("metalake", "catalog"));

    CatalogConnectorContext mockContext = mock(CatalogConnectorContext.class);
    when(mockContext.getCatalog()).thenReturn(mockCatalog);
    GravitinoMetalake metalake = mockMetalake();
    when(mockContext.getMetalake()).thenReturn(metalake);

    Connector mockInternalConnector = mock(Connector.class);
    when(mockContext.getInternalConnector()).thenReturn(mockInternalConnector);
    when(mockInternalConnector.beginTransaction(any(), anyBoolean(), anyBoolean()))
        .thenReturn(null);
    GravitinoConnector connector = new GravitinoConnector(mockContext);
    assertThrows(
        IllegalArgumentException.class,
        () -> connector.beginTransaction(IsolationLevel.READ_COMMITTED, true, true));
  }

  private static GravitinoMetalake mockMetalake() {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(metalake.loadCatalog(any())).thenReturn(catalog);
    return metalake;
  }
}
