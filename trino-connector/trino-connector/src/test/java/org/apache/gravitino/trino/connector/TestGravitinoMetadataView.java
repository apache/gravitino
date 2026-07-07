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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoView;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataView {

  @Test
  public void testListViewsForSingleSchema() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.listViews("test_schema")).thenReturn(List.of("v1", "v2"));

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    List<SchemaTableName> views = metadata.listViews(session, Optional.of("test_schema"));
    assertEquals(2, views.size());
    assertTrue(views.contains(new SchemaTableName("test_schema", "v1")));
    assertTrue(views.contains(new SchemaTableName("test_schema", "v2")));
  }

  @Test
  public void testListViewsAcrossAllSchemas() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.listSchemaNames()).thenReturn(List.of("s1"));
    when(catalogMetadata.listViews("s1")).thenReturn(List.of("v1"));

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    List<SchemaTableName> views = metadata.listViews(session, Optional.empty());
    assertEquals(1, views.size());
    assertEquals(new SchemaTableName("s1", "v1"), views.get(0));
  }

  @Test
  public void testGetViewReturnsEmptyWhenNotPresent() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.getViewIfPresent("s", "v1")).thenReturn(Optional.empty());

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Optional<ConnectorViewDefinition> definition =
        metadata.getView(session, new SchemaTableName("s", "v1"));
    assertTrue(definition.isEmpty());
  }

  @Test
  public void testGetViewReturnsDefinitionWhenPresent() {
    GravitinoView view =
        new GravitinoView("s", "v1", List.of(), null, Map.of(), "select 1", null, null);
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.getViewIfPresent("s", "v1")).thenReturn(Optional.of(view));

    ConnectorViewDefinition connectorViewDefinition = mock(ConnectorViewDefinition.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    when(metadataAdapter.getViewDefinition(view)).thenReturn(connectorViewDefinition);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata, metadataAdapter);
    ConnectorSession session = mock(ConnectorSession.class);

    Optional<ConnectorViewDefinition> definition =
        metadata.getView(session, new SchemaTableName("s", "v1"));
    assertTrue(definition.isPresent());
    assertEquals(connectorViewDefinition, definition.get());
  }

  @Test
  public void testDropViewDelegates() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    metadata.dropView(session, new SchemaTableName("s", "v1"));
    verify(catalogMetadata, times(1)).dropView("s", "v1");
  }

  @Test
  public void testRenameViewDelegates() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    SchemaTableName source = new SchemaTableName("s", "v1");
    SchemaTableName target = new SchemaTableName("s", "v2");
    metadata.renameView(session, source, target);
    verify(catalogMetadata, times(1)).renameView(source, target);
  }

  @Test
  public void testCreateViewInternalDelegatesToAdapterAndCatalogMetadata() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorViewDefinition definition = mock(ConnectorViewDefinition.class);
    SchemaTableName viewName = new SchemaTableName("s", "v1");
    Map<String, Object> viewProperties = Map.of("k", "v");
    GravitinoView view =
        new GravitinoView("s", "v1", List.of(), null, Map.of(), "select 1", null, null);
    when(metadataAdapter.createView(viewName, definition, viewProperties)).thenReturn(view);

    ExposedGravitinoMetadata metadata = createTestMetadata(catalogMetadata, metadataAdapter);
    metadata.exposedCreateViewInternal(viewName, definition, viewProperties, true);

    verify(catalogMetadata, times(1)).createView(eq(view), eq(true));
  }

  @Test
  public void testCreateViewInternalNotReplace() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorViewDefinition definition = mock(ConnectorViewDefinition.class);
    SchemaTableName viewName = new SchemaTableName("s", "v1");
    GravitinoView view =
        new GravitinoView("s", "v1", List.of(), null, Map.of(), "select 1", null, null);
    when(metadataAdapter.createView(viewName, definition, Map.of())).thenReturn(view);

    ExposedGravitinoMetadata metadata = createTestMetadata(catalogMetadata, metadataAdapter);
    metadata.exposedCreateViewInternal(viewName, definition, Map.of(), false);

    verify(catalogMetadata, times(1)).createView(eq(view), eq(false));
    verify(catalogMetadata, never()).createView(any(), eq(true));
  }

  private ExposedGravitinoMetadata createTestMetadata(CatalogConnectorMetadata catalogMetadata) {
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    return createTestMetadata(catalogMetadata, metadataAdapter);
  }

  private ExposedGravitinoMetadata createTestMetadata(
      CatalogConnectorMetadata catalogMetadata, CatalogConnectorMetadataAdapter metadataAdapter) {
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    return new ExposedGravitinoMetadata(catalogMetadata, metadataAdapter, internalMetadata);
  }

  /**
   * Test-only subclass that exposes the protected {@code createViewInternal} helper for direct
   * verification, since {@link GravitinoMetadata} is abstract and the helper is only otherwise
   * reachable through the per-Trino-version {@code createView} overrides.
   */
  private static final class ExposedGravitinoMetadata extends GravitinoMetadata {
    ExposedGravitinoMetadata(
        CatalogConnectorMetadata catalogConnectorMetadata,
        CatalogConnectorMetadataAdapter metadataAdapter,
        ConnectorMetadata internalMetadata) {
      super(catalogConnectorMetadata, metadataAdapter, internalMetadata);
    }

    void exposedCreateViewInternal(
        SchemaTableName viewName,
        ConnectorViewDefinition definition,
        Map<String, Object> viewProperties,
        boolean replace) {
      createViewInternal(viewName, definition, viewProperties, replace);
    }
  }
}
