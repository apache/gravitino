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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataGetSystemTable {

  @Test
  public void testGetSystemTableDelegatesToInternalMetadata() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);

    SystemTable mockSystemTable = mock(SystemTable.class);
    SchemaTableName tableName = new SchemaTableName("test_schema", "test_table$snapshots");

    when(internalMetadata.getSystemTable(any(ConnectorSession.class), any(SchemaTableName.class)))
        .thenReturn(Optional.of(mockSystemTable));

    GravitinoMetadata gravitinoMetadata =
        new GravitinoMetadata(catalogConnectorMetadata, metadataAdapter, internalMetadata);

    Optional<SystemTable> result = gravitinoMetadata.getSystemTable(session, tableName);

    assertTrue(result.isPresent());
    assertEquals(mockSystemTable, result.get());
    verify(internalMetadata).getSystemTable(session, tableName);
  }

  @Test
  public void testGetSystemTableReturnsEmptyWhenInternalReturnsEmpty() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);

    SchemaTableName tableName = new SchemaTableName("test_schema", "test_table$invalid");

    when(internalMetadata.getSystemTable(any(ConnectorSession.class), any(SchemaTableName.class)))
        .thenReturn(Optional.empty());

    GravitinoMetadata gravitinoMetadata =
        new GravitinoMetadata(catalogConnectorMetadata, metadataAdapter, internalMetadata);

    Optional<SystemTable> result = gravitinoMetadata.getSystemTable(session, tableName);

    assertFalse(result.isPresent());
    verify(internalMetadata).getSystemTable(session, tableName);
  }
}
