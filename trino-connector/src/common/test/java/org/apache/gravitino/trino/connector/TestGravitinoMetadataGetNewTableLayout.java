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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataGetNewTableLayout {

  @Test
  public void testGetNewTableLayoutReturnsEmptyOnClassCastException() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("test_schema", "test_table"), Collections.emptyList());

    when(internalMetadata.getNewTableLayout(
            any(ConnectorSession.class), any(ConnectorTableMetadata.class)))
        .thenThrow(new ClassCastException("String cannot be cast to HiveStorageFormat"));

    GravitinoMetadata metadata =
        new StubGravitinoMetadata(catalogConnectorMetadata, metadataAdapter, internalMetadata);

    Optional<ConnectorTableLayout> result = metadata.getNewTableLayout(session, tableMetadata);

    assertFalse(result.isPresent());
  }

  @Test
  public void testGetNewTableLayoutDelegatesToInternalWhenCompatible() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("test_schema", "test_table"), Collections.emptyList());

    when(internalMetadata.getNewTableLayout(
            any(ConnectorSession.class), any(ConnectorTableMetadata.class)))
        .thenReturn(Optional.empty());

    GravitinoMetadata metadata =
        new StubGravitinoMetadata(catalogConnectorMetadata, metadataAdapter, internalMetadata);

    Optional<ConnectorTableLayout> result = metadata.getNewTableLayout(session, tableMetadata);

    assertFalse(result.isPresent());
  }

  @Test
  public void testGetNewTableLayoutReturnsLayoutWithPartitionColumns() {
    CatalogConnectorMetadata catalogConnectorMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    ConnectorSession session = mock(ConnectorSession.class);

    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("test_schema", "test_table"), Collections.emptyList());

    ConnectorTableLayout internalLayout = new ConnectorTableLayout(Collections.singletonList("id"));
    when(internalMetadata.getNewTableLayout(
            any(ConnectorSession.class), any(ConnectorTableMetadata.class)))
        .thenReturn(Optional.of(internalLayout));

    GravitinoMetadata metadata =
        new StubGravitinoMetadata(catalogConnectorMetadata, metadataAdapter, internalMetadata);

    Optional<ConnectorTableLayout> result = metadata.getNewTableLayout(session, tableMetadata);

    assertTrue(result.isPresent());
  }

  private static final class StubGravitinoMetadata extends GravitinoMetadata {
    private StubGravitinoMetadata(
        CatalogConnectorMetadata catalogConnectorMetadata,
        CatalogConnectorMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }
  }
}
