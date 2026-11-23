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

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataGetTableHandle {

  private static final SchemaTableName NAME = new SchemaTableName("s", "v");

  @Test
  public void testViewIsReportedAsNotSupported() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogConnectorMetadata.tableExists("s", "v")).thenReturn(false);
    when(catalogConnectorMetadata.viewExists("s", "v")).thenReturn(true);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    GravitinoMetadata metadata = createMetadata(catalogConnectorMetadata, internalMetadata);

    TrinoException e =
        assertThrows(
            TrinoException.class,
            () ->
                metadata.getTableHandle(
                    mock(ConnectorSession.class), NAME, Optional.empty(), Optional.empty()));
    assertEquals(NOT_SUPPORTED.toErrorCode(), e.getErrorCode());
    assertTrue(e.getMessage().contains("is a view"));
    verify(internalMetadata, never()).getTableHandle(any(), any(), any(), any());
  }

  @Test
  public void testMissingTableAndViewReturnsNull() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogConnectorMetadata.tableExists("s", "v")).thenReturn(false);
    when(catalogConnectorMetadata.viewExists("s", "v")).thenReturn(false);
    GravitinoMetadata metadata =
        createMetadata(catalogConnectorMetadata, mock(ConnectorMetadata.class));

    assertNull(
        metadata.getTableHandle(
            mock(ConnectorSession.class), NAME, Optional.empty(), Optional.empty()));
  }

  @Test
  public void testExistingTableDoesNotCheckViews() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogConnectorMetadata.tableExists("s", "v")).thenReturn(true);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    when(internalMetadata.getTableHandle(any(), any(), any(), any()))
        .thenReturn(mock(ConnectorTableHandle.class));
    GravitinoMetadata metadata = createMetadata(catalogConnectorMetadata, internalMetadata);

    metadata.getTableHandle(mock(ConnectorSession.class), NAME, Optional.empty(), Optional.empty());
    verify(catalogConnectorMetadata, never()).viewExists(any(), any());
  }

  private static GravitinoMetadata createMetadata(
      CatalogConnectorMetadata catalogConnectorMetadata, ConnectorMetadata internalMetadata) {
    return new TestGravitinoMetadata(
        catalogConnectorMetadata, mock(CatalogConnectorMetadataAdapter.class), internalMetadata);
  }

  private static final class TestGravitinoMetadata extends GravitinoMetadata {
    private TestGravitinoMetadata(
        CatalogConnectorMetadata catalogConnectorMetadata,
        CatalogConnectorMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }
  }
}
