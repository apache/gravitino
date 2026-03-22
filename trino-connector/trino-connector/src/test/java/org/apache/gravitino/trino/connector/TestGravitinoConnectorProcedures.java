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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

/**
 * Tests that verify the Gravitino Trino connector properly delegates procedure-related operations
 * to the internal connector, enabling support for Iceberg snapshot maintenance procedures.
 */
public class TestGravitinoConnectorProcedures {

  @Test
  void testGetProceduresDelegatesToInternalConnector() {
    Connector mockInternalConnector = mock(Connector.class);
    Set<Procedure> expectedProcedures = Set.of(mock(Procedure.class), mock(Procedure.class));
    when(mockInternalConnector.getProcedures()).thenReturn(expectedProcedures);

    GravitinoConnector connector = createConnector(mockInternalConnector);
    Set<Procedure> result = connector.getProcedures();

    assertEquals(expectedProcedures, result);
    verify(mockInternalConnector).getProcedures();
  }

  @Test
  void testGetProceduresReturnsEmptySetWhenNoProcedures() {
    Connector mockInternalConnector = mock(Connector.class);
    when(mockInternalConnector.getProcedures()).thenReturn(Set.of());

    GravitinoConnector connector = createConnector(mockInternalConnector);
    Set<Procedure> result = connector.getProcedures();

    assertTrue(result.isEmpty());
  }

  @Test
  void testGetTableProceduresDelegatesToInternalConnector() {
    Connector mockInternalConnector = mock(Connector.class);
    Set<TableProcedureMetadata> expectedProcedures = Set.of(mock(TableProcedureMetadata.class));
    when(mockInternalConnector.getTableProcedures()).thenReturn(expectedProcedures);

    GravitinoConnector connector = createConnector(mockInternalConnector);
    Set<TableProcedureMetadata> result = connector.getTableProcedures();

    assertEquals(expectedProcedures, result);
    verify(mockInternalConnector).getTableProcedures();
  }

  @Test
  void testGetTableHandleForExecuteDelegatesToInternalMetadata() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableHandle internalTableHandle = mock(ConnectorTableHandle.class);
    ConnectorTableExecuteHandle expectedExecuteHandle = mock(ConnectorTableExecuteHandle.class);

    GravitinoTableHandle gravitinoTableHandle =
        new GravitinoTableHandle("test_schema", "test_table", internalTableHandle);

    Map<String, Object> executeProperties = Map.of();
    when(internalMetadata.getTableHandleForExecute(
            any(), eq(internalTableHandle), anyString(), any(), any()))
        .thenReturn(Optional.of(expectedExecuteHandle));

    GravitinoMetadata metadata = createMetadata(internalMetadata);

    Optional<ConnectorTableExecuteHandle> result =
        metadata.getTableHandleForExecute(
            session, gravitinoTableHandle, "optimize", executeProperties, RetryMode.NO_RETRIES);

    assertTrue(result.isPresent());
    assertSame(expectedExecuteHandle, result.get());
    verify(internalMetadata)
        .getTableHandleForExecute(
            session, internalTableHandle, "optimize", executeProperties, RetryMode.NO_RETRIES);
  }

  @Test
  void testBeginTableExecuteUnwrapsAndWrapsHandles() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableHandle internalTableHandle = mock(ConnectorTableHandle.class);
    ConnectorTableExecuteHandle executeHandle = mock(ConnectorTableExecuteHandle.class);
    ConnectorTableHandle resultSourceHandle = mock(ConnectorTableHandle.class);
    ConnectorTableExecuteHandle resultExecuteHandle = mock(ConnectorTableExecuteHandle.class);

    GravitinoTableHandle gravitinoTableHandle =
        new GravitinoTableHandle("test_schema", "test_table", internalTableHandle);

    BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> internalResult =
        new BeginTableExecuteResult<>(resultExecuteHandle, resultSourceHandle);

    when(internalMetadata.beginTableExecute(session, executeHandle, internalTableHandle))
        .thenReturn(internalResult);

    GravitinoMetadata metadata = createMetadata(internalMetadata);

    BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> result =
        metadata.beginTableExecute(session, executeHandle, gravitinoTableHandle);

    assertSame(resultExecuteHandle, result.getTableExecuteHandle());
    assertTrue(result.getSourceHandle() instanceof GravitinoTableHandle);
    GravitinoTableHandle wrappedSource = (GravitinoTableHandle) result.getSourceHandle();
    assertEquals("test_schema", wrappedSource.getSchemaName());
    assertEquals("test_table", wrappedSource.getTableName());
    assertSame(resultSourceHandle, wrappedSource.getInternalHandle());
  }

  @Test
  void testFinishTableExecuteDelegatesToInternalMetadata() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle executeHandle = mock(ConnectorTableExecuteHandle.class);

    GravitinoMetadata metadata = createMetadata(internalMetadata);
    metadata.finishTableExecute(session, executeHandle, java.util.List.of(), java.util.List.of());

    verify(internalMetadata).finishTableExecute(eq(session), eq(executeHandle), any(), any());
  }

  @Test
  void testExecuteTableExecuteDelegatesToInternalMetadata() {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);
    ConnectorTableExecuteHandle executeHandle = mock(ConnectorTableExecuteHandle.class);

    GravitinoMetadata metadata = createMetadata(internalMetadata);
    metadata.executeTableExecute(session, executeHandle);

    verify(internalMetadata).executeTableExecute(session, executeHandle);
  }

  private GravitinoConnector createConnector(Connector internalConnector) {
    GravitinoCatalog mockCatalog = mock(GravitinoCatalog.class);
    when(mockCatalog.geNameIdentifier()).thenReturn(NameIdentifier.of("metalake", "catalog"));

    GravitinoMetalake metalake = mockMetalake();

    CatalogConnectorContext mockContext = mock(CatalogConnectorContext.class);
    when(mockContext.getCatalog()).thenReturn(mockCatalog);
    when(mockContext.getMetalake()).thenReturn(metalake);
    when(mockContext.getInternalConnector()).thenReturn(internalConnector);

    return new GravitinoConnector(mockContext);
  }

  private GravitinoMetadata createMetadata(ConnectorMetadata internalMetadata) {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    return new TestGravitinoMetadataImpl(
        catalogConnectorMetadata, metadataAdapter, internalMetadata);
  }

  private static GravitinoMetalake mockMetalake() {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(metalake.loadCatalog(any())).thenReturn(catalog);
    return metalake;
  }

  private static final class TestGravitinoMetadataImpl extends GravitinoMetadata {
    private TestGravitinoMetadataImpl(
        CatalogConnectorMetadata catalogConnectorMetadata,
        CatalogConnectorMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
        ConnectorSession session,
        ConnectorTableHandle tableHandle,
        String procedureName,
        Map<String, Object> executeProperties,
        RetryMode retryMode) {
      return internalMetadata.getTableHandleForExecute(
          session,
          GravitinoHandle.unWrap(tableHandle),
          procedureName,
          executeProperties,
          retryMode);
    }

    @Override
    public void executeTableExecute(
        ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle) {
      internalMetadata.executeTableExecute(session, tableExecuteHandle);
    }
  }
}
