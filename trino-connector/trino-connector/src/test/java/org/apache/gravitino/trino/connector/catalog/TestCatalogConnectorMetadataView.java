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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.metadata.GravitinoView;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestCatalogConnectorMetadataView {

  @Test
  public void testSupportsViewsWhenCatalogSupports() {
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(mock(ViewCatalog.class));
    assertTrue(metadata.supportsViews());
  }

  @Test
  public void testSupportsViewsWhenCatalogDoesNotSupport() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    assertFalse(metadata.supportsViews());
  }

  @Test
  public void testGetViewIfPresentReturnsEmptyWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    assertTrue(metadata.getViewIfPresent("db", "v1").isEmpty());
  }

  @Test
  public void testGetViewIfPresentReturnsEmptyWhenNotFound() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.loadView(any())).thenThrow(new NoSuchViewException("not found"));

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    assertTrue(metadata.getViewIfPresent("db", "v1").isEmpty());
  }

  @Test
  public void testGetViewIfPresentReturnsEmptyWhenViewOperationUnsupported() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.loadView(any())).thenThrow(new UnsupportedOperationException("unsupported"));

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    assertTrue(metadata.getViewIfPresent("db", "v1").isEmpty());
  }

  @Test
  public void testGetViewIfPresentReturnsEmptyWhenNoTrinoRepresentation() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.HIVE, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    assertTrue(metadata.getViewIfPresent("db", "v1").isEmpty());
  }

  @Test
  public void testGetViewIfPresentReturnsValueWhenTrinoRepresentationExists() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View mockView = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(mockView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    Optional<GravitinoView> view = metadata.getViewIfPresent("db", "v1");
    assertTrue(view.isPresent());
    assertEquals("select 1", view.get().getSql());
  }

  @Test
  public void testGetViewThrowsWhenNotPresent() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.getView("db", "v1"));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_VIEW_NOT_EXISTS.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testListViewsReturnsEmptyWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    assertTrue(metadata.listViews("db").isEmpty());
  }

  @Test
  public void testListViewsReturnsEmptyWhenViewOperationUnsupported() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.listViews(any(Namespace.class)))
        .thenThrow(new UnsupportedOperationException("unsupported"));

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    assertTrue(metadata.listViews("db").isEmpty());
  }

  @Test
  public void testListViewsReturnsNames() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.listViews(any(Namespace.class)))
        .thenReturn(new NameIdentifier[] {NameIdentifier.of("db", "v1")});
    when(viewCatalog.loadView(NameIdentifier.of("db", "v1"))).thenReturn(view);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    List<String> views = metadata.listViews("db");
    assertEquals(1, views.size());
    assertEquals("v1", views.get(0));
  }

  @Test
  public void testListViewsFiltersViewsWithoutTrinoRepresentation() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View trinoView = createView(Dialects.TRINO, "select 1");
    View hiveView = createView(Dialects.HIVE, "select 2");
    when(viewCatalog.listViews(any(Namespace.class)))
        .thenReturn(
            new NameIdentifier[] {NameIdentifier.of("db", "v1"), NameIdentifier.of("db", "v2")});
    when(viewCatalog.loadView(NameIdentifier.of("db", "v1"))).thenReturn(trinoView);
    when(viewCatalog.loadView(NameIdentifier.of("db", "v2"))).thenReturn(hiveView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    assertEquals(List.of("v1"), metadata.listViews("db"));
  }

  @Test
  public void testCreateViewThrowsWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    assertThrows(TrinoException.class, () -> metadata.createView(view, false));
  }

  @Test
  public void testCreateViewThrowsWhenUnderlyingCatalogUnsupported() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(false);
    when(viewCatalog.createView(any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new UnsupportedOperationException("createView is not supported"));

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.createView(view, false));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testCreateViewCreatesWhenNotExists() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(false);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    metadata.createView(view, false);

    verify(viewCatalog, times(1))
        .createView(eq(NameIdentifier.of("db", "v1")), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testCreateViewThrowsWhenExistsAndNotReplace() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(true);
    View existingView = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(existingView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.createView(view, false));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_VIEW_ALREADY_EXISTS.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testCreateViewReplacesWhenExistsAndReplace() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(true);
    View existingView = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(existingView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    metadata.createView(view, true);

    verify(viewCatalog, times(1))
        .alterView(eq(NameIdentifier.of("db", "v1")), any(ViewChange.ReplaceView.class));
  }

  @Test
  public void testCreateViewReplacePreservesNonTrinoRepresentations() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(true);
    View existingView = mock(View.class);
    when(existingView.columns()).thenReturn(new Column[0]);
    Representation[] existingRepresentations = {
      SQLRepresentation.builder().withDialect(Dialects.TRINO).withSql("select 1").build(),
      SQLRepresentation.builder().withDialect(Dialects.HIVE).withSql("select 1 from t").build()
    };
    when(existingView.representations()).thenReturn(existingRepresentations);
    when(existingView.sqlFor(Dialects.TRINO))
        .thenReturn(Optional.of((SQLRepresentation) existingRepresentations[0]));
    when(viewCatalog.loadView(any())).thenReturn(existingView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 2");
    metadata.createView(view, true);

    ArgumentCaptor<ViewChange.ReplaceView> captor =
        ArgumentCaptor.forClass(ViewChange.ReplaceView.class);
    verify(viewCatalog, times(1)).alterView(eq(NameIdentifier.of("db", "v1")), captor.capture());
    Representation[] newRepresentations = captor.getValue().getRepresentations();
    assertEquals(2, newRepresentations.length);
    assertTrue(
        Arrays.stream(newRepresentations)
            .anyMatch(
                r ->
                    r instanceof SQLRepresentation
                        && Dialects.HIVE.equalsIgnoreCase(((SQLRepresentation) r).dialect())
                        && "select 1 from t".equals(((SQLRepresentation) r).sql())));
    assertTrue(
        Arrays.stream(newRepresentations)
            .anyMatch(
                r ->
                    r instanceof SQLRepresentation
                        && Dialects.TRINO.equalsIgnoreCase(((SQLRepresentation) r).dialect())
                        && "select 2".equals(((SQLRepresentation) r).sql())));
  }

  @Test
  public void testCreateViewThrowsWhenExistsWithoutTrinoRepresentationAndReplace() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    when(viewCatalog.viewExists(any())).thenReturn(true);
    View existingView = createView(Dialects.HIVE, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(existingView);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    GravitinoView view = createGravitinoView("db", "v1", "select 1");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.createView(view, true));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_VIEW_ALREADY_EXISTS.toErrorCode(), exception.getErrorCode());
    verify(viewCatalog, never()).alterView(any(), any(ViewChange.ReplaceView.class));
  }

  @Test
  public void testDropViewThrowsWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.dropView("db", "v1"));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testDropView() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    when(viewCatalog.dropView(any())).thenReturn(true);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    metadata.dropView("db", "v1");
    verify(viewCatalog, times(1)).dropView(NameIdentifier.of("db", "v1"));
  }

  @Test
  public void testDropViewThrowsWhenFailed() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    when(viewCatalog.dropView(any())).thenReturn(false);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    assertThrows(TrinoException.class, () -> metadata.dropView("db", "v1"));
  }

  @Test
  public void testDropViewThrowsWhenUnderlyingCatalogUnsupported() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    when(viewCatalog.dropView(any()))
        .thenThrow(new UnsupportedOperationException("dropView is not supported"));

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.dropView("db", "v1"));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testDropViewThrowsWhenNoTrinoRepresentation() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.HIVE, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);

    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.dropView("db", "v1"));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_VIEW_NOT_EXISTS.toErrorCode(), exception.getErrorCode());
    verify(viewCatalog, never()).dropView(any());
  }

  @Test
  public void testRenameViewThrowsWhenUnsupported() {
    CatalogConnectorMetadata metadata = createMetadataWithoutViewCatalog();
    SchemaTableName oldName = new SchemaTableName("db", "v1");
    SchemaTableName newName = new SchemaTableName("db", "v2");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.renameView(oldName, newName));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testRenameViewRejectsCrossSchema() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    SchemaTableName oldName = new SchemaTableName("db1", "v1");
    SchemaTableName newName = new SchemaTableName("db2", "v1");
    assertThrows(TrinoException.class, () -> metadata.renameView(oldName, newName));
    verify(viewCatalog, never()).alterView(any(), any());
  }

  @Test
  public void testRenameViewSameNameIsNoop() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    SchemaTableName oldName = new SchemaTableName("db", "v1");
    SchemaTableName newName = new SchemaTableName("db", "v1");
    metadata.renameView(oldName, newName);
    verify(viewCatalog, never()).alterView(any(), any());
  }

  @Test
  public void testRenameViewDelegatesToAlterView() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    SchemaTableName oldName = new SchemaTableName("db", "v1");
    SchemaTableName newName = new SchemaTableName("db", "v2");
    metadata.renameView(oldName, newName);
    verify(viewCatalog, times(1))
        .alterView(eq(NameIdentifier.of("db", "v1")), any(ViewChange.RenameView.class));
  }

  @Test
  public void testRenameViewThrowsWhenUnderlyingCatalogUnsupported() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.TRINO, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    when(viewCatalog.alterView(any(), any(ViewChange.RenameView.class)))
        .thenThrow(new UnsupportedOperationException("alterView is not supported"));
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    SchemaTableName oldName = new SchemaTableName("db", "v1");
    SchemaTableName newName = new SchemaTableName("db", "v2");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.renameView(oldName, newName));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION.toErrorCode(), exception.getErrorCode());
  }

  @Test
  public void testRenameViewThrowsWhenNoTrinoRepresentation() {
    ViewCatalog viewCatalog = mock(ViewCatalog.class);
    View view = createView(Dialects.HIVE, "select 1");
    when(viewCatalog.loadView(any())).thenReturn(view);
    CatalogConnectorMetadata metadata = createMetadataWithViewCatalog(viewCatalog);

    SchemaTableName oldName = new SchemaTableName("db", "v1");
    SchemaTableName newName = new SchemaTableName("db", "v2");
    TrinoException exception =
        assertThrows(TrinoException.class, () -> metadata.renameView(oldName, newName));
    assertEquals(
        GravitinoErrorCode.GRAVITINO_VIEW_NOT_EXISTS.toErrorCode(), exception.getErrorCode());
    verify(viewCatalog, never()).alterView(any(), any());
  }

  private static View createView(String dialect, String sql) {
    View view = mock(View.class);
    when(view.columns()).thenReturn(new Column[0]);
    when(view.comment()).thenReturn(null);
    when(view.properties()).thenReturn(Collections.emptyMap());
    Representation[] representations = {
      SQLRepresentation.builder().withDialect(dialect).withSql(sql).build()
    };
    when(view.representations()).thenReturn(representations);
    when(view.sqlFor(anyString()))
        .thenAnswer(
            invocation -> {
              String requestedDialect = invocation.getArgument(0);
              if (requestedDialect.equalsIgnoreCase(dialect)) {
                return Optional.of(representations[0]);
              }
              return Optional.empty();
            });
    when(view.defaultCatalog()).thenReturn(null);
    when(view.defaultSchema()).thenReturn(null);
    return view;
  }

  private static GravitinoView createGravitinoView(String schemaName, String viewName, String sql) {
    return new GravitinoView(schemaName, viewName, List.of(), null, Map.of(), sql, null, null);
  }

  private CatalogConnectorMetadata createMetadataWithViewCatalog(ViewCatalog viewCatalog) {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn("test_catalog");
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(catalog.asViewCatalog()).thenReturn(viewCatalog);
    return new CatalogConnectorMetadata(metalake, NameIdentifier.of("metalake", "test_catalog"));
  }

  private CatalogConnectorMetadata createMetadataWithoutViewCatalog() {
    GravitinoMetalake metalake = mock(GravitinoMetalake.class);
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn("test_catalog");
    when(metalake.loadCatalog(anyString())).thenReturn(catalog);
    when(catalog.asSchemas()).thenReturn(mock(SupportsSchemas.class));
    when(catalog.asTableCatalog()).thenReturn(mock(TableCatalog.class));
    when(catalog.asViewCatalog()).thenThrow(new UnsupportedOperationException("Not supported"));
    return new CatalogConnectorMetadata(metalake, NameIdentifier.of("metalake", "test_catalog"));
  }
}
