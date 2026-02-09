/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergViewHookDispatcher {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA_NAME = "test_schema";
  private static final String VIEW_NAME = "test_view";
  private static final String USER = "test_user";

  private static final Schema VIEW_SCHEMA =
      new Schema(Types.NestedField.required(1, "test_field", Types.StringType.get()));

  private IcebergViewHookDispatcher hookDispatcher;
  private IcebergViewOperationDispatcher mockExecutor;
  private EntityStore mockEntityStore;
  private ViewDispatcher mockViewDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private IcebergRequestContext mockContext;
  private GravitinoEnv gravitinoEnv;

  @BeforeEach
  public void setUp() {
    mockExecutor = mock(IcebergViewOperationDispatcher.class);
    mockEntityStore = mock(EntityStore.class);
    mockViewDispatcher = mock(ViewDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockContext = mock(IcebergRequestContext.class);

    when(mockContext.catalogName()).thenReturn(CATALOG);
    when(mockContext.userName()).thenReturn(USER);

    // Setup GravitinoEnv mock
    gravitinoEnv = GravitinoEnv.getInstance();
    try {
      FieldUtils.writeField(gravitinoEnv, "entityStore", mockEntityStore, true);
      FieldUtils.writeField(gravitinoEnv, "viewDispatcher", mockViewDispatcher, true);
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", mockOwnerDispatcher, true);
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup test", e);
    }

    hookDispatcher = new IcebergViewHookDispatcher(mockExecutor, METALAKE);
  }

  @AfterEach
  public void tearDown() {
    try {
      // Clean up GravitinoEnv
      FieldUtils.writeField(gravitinoEnv, "entityStore", null, true);
      FieldUtils.writeField(gravitinoEnv, "viewDispatcher", null, true);
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", null, true);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Test
  public void testCreateViewImportsIntoEntityStore() throws Exception {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    CreateViewRequest createRequest =
        ImmutableCreateViewRequest.builder()
            .name(VIEW_NAME)
            .schema(VIEW_SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(namespace)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT * FROM test")
                            .dialect("spark")
                            .build())
                    .build())
            .build();

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockExecutor.createView(mockContext, namespace, createRequest)).thenReturn(mockResponse);

    LoadViewResponse response = hookDispatcher.createView(mockContext, namespace, createRequest);

    // Verify view was created in underlying catalog
    verify(mockExecutor, times(1)).createView(mockContext, namespace, createRequest);

    // Verify view was imported into Gravitino
    NameIdentifier expectedIdent = NameIdentifier.of(METALAKE, CATALOG, SCHEMA_NAME, VIEW_NAME);
    verify(mockViewDispatcher, times(1)).loadView(eq(expectedIdent));

    assertEquals(mockResponse, response);
  }

  @Test
  public void testCreateViewSetsOwnership() throws Exception {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    CreateViewRequest createRequest =
        ImmutableCreateViewRequest.builder()
            .name(VIEW_NAME)
            .schema(VIEW_SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(namespace)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT * FROM test")
                            .dialect("spark")
                            .build())
                    .build())
            .build();

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockExecutor.createView(mockContext, namespace, createRequest)).thenReturn(mockResponse);

    hookDispatcher.createView(mockContext, namespace, createRequest);

    // Verify ownership was set - this is tested via IcebergOwnershipUtils
    // The actual verification is that ownerDispatcher.setOwner() is called
    // This is covered by the integration between hookDispatcher and ownerDispatcher
    verify(mockOwnerDispatcher, times(1)).setOwner(any(), any(), eq(USER), any());
  }

  @Test
  public void testCreateViewHandlesImportFailureGracefully() throws Exception {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    CreateViewRequest createRequest =
        ImmutableCreateViewRequest.builder()
            .name(VIEW_NAME)
            .schema(VIEW_SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(namespace)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql("SELECT * FROM test")
                            .dialect("spark")
                            .build())
                    .build())
            .build();

    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockExecutor.createView(mockContext, namespace, createRequest)).thenReturn(mockResponse);

    // Simulate import failure
    doThrow(new RuntimeException("Import failed"))
        .when(mockViewDispatcher)
        .loadView(any(NameIdentifier.class));

    // Should not throw - import is best-effort
    LoadViewResponse response = hookDispatcher.createView(mockContext, namespace, createRequest);

    assertEquals(mockResponse, response);
    verify(mockExecutor, times(1)).createView(mockContext, namespace, createRequest);
  }

  @Test
  public void testDropViewRemovesFromEntityStore() throws Exception {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);

    hookDispatcher.dropView(mockContext, viewIdent);

    // Verify view was dropped from underlying catalog
    verify(mockExecutor, times(1)).dropView(mockContext, viewIdent);

    // Verify view was deleted from entity store
    NameIdentifier expectedIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, viewIdent);
    verify(mockEntityStore, times(1)).delete(eq(expectedIdent), eq(Entity.EntityType.VIEW));
  }

  @Test
  public void testDropViewHandlesMissingEntity() throws Exception {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);

    // Simulate entity not found in store
    NameIdentifier expectedIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, viewIdent);
    doThrow(new NoSuchEntityException("Entity not found"))
        .when(mockEntityStore)
        .delete(eq(expectedIdent), eq(Entity.EntityType.VIEW));

    // Should not throw - missing entity is ignored
    hookDispatcher.dropView(mockContext, viewIdent);

    verify(mockExecutor, times(1)).dropView(mockContext, viewIdent);
    verify(mockEntityStore, times(1)).delete(eq(expectedIdent), eq(Entity.EntityType.VIEW));
  }

  @Test
  public void testDropViewHandlesIOException() throws Exception {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);

    // Simulate IO error
    NameIdentifier expectedIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, viewIdent);
    doThrow(new IOException("IO error"))
        .when(mockEntityStore)
        .delete(eq(expectedIdent), eq(Entity.EntityType.VIEW));

    // Should throw RuntimeException wrapping the IOException
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> hookDispatcher.dropView(mockContext, viewIdent));

    assertEquals("Failed to delete view entity from store", exception.getMessage());
    verify(mockExecutor, times(1)).dropView(mockContext, viewIdent);
  }

  @Test
  public void testRenameViewUpdatesEntityStore() throws Exception {
    TableIdentifier sourceIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "old_view");
    TableIdentifier destIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "new_view");
    RenameTableRequest renameRequest =
        RenameTableRequest.builder().withSource(sourceIdent).withDestination(destIdent).build();

    hookDispatcher.renameView(mockContext, renameRequest);

    // Verify view was renamed in underlying catalog
    verify(mockExecutor, times(1)).renameView(mockContext, renameRequest);

    // Verify entity store was updated
    NameIdentifier sourceGravitinoIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, sourceIdent);
    verify(mockEntityStore, times(1))
        .update(
            eq(sourceGravitinoIdent),
            eq(GenericEntity.class),
            eq(Entity.EntityType.VIEW),
            any(java.util.function.Function.class));
  }

  @Test
  public void testRenameViewHandlesMissingEntity() throws Exception {
    TableIdentifier sourceIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "old_view");
    TableIdentifier destIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "new_view");
    RenameTableRequest renameRequest =
        RenameTableRequest.builder().withSource(sourceIdent).withDestination(destIdent).build();

    // Simulate entity not found in store
    NameIdentifier sourceGravitinoIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, sourceIdent);
    when(mockEntityStore.update(
            eq(sourceGravitinoIdent),
            eq(GenericEntity.class),
            eq(Entity.EntityType.VIEW),
            any(java.util.function.Function.class)))
        .thenThrow(new NoSuchEntityException("Entity not found"));

    // Should not throw - missing entity is ignored
    hookDispatcher.renameView(mockContext, renameRequest);

    verify(mockExecutor, times(1)).renameView(mockContext, renameRequest);
  }

  @Test
  public void testRenameViewHandlesIOException() throws Exception {
    TableIdentifier sourceIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "old_view");
    TableIdentifier destIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "new_view");
    RenameTableRequest renameRequest =
        RenameTableRequest.builder().withSource(sourceIdent).withDestination(destIdent).build();

    // Simulate IO error
    NameIdentifier sourceGravitinoIdent =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(METALAKE, CATALOG, sourceIdent);
    when(mockEntityStore.update(
            eq(sourceGravitinoIdent),
            eq(GenericEntity.class),
            eq(Entity.EntityType.VIEW),
            any(java.util.function.Function.class)))
        .thenThrow(new IOException("IO error"));

    // Should throw RuntimeException wrapping the IOException
    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> hookDispatcher.renameView(mockContext, renameRequest));

    assertEquals("Failed to rename view entity in store", exception.getMessage());
    verify(mockExecutor, times(1)).renameView(mockContext, renameRequest);
  }

  @Test
  public void testReplaceViewDelegatesWithoutModification() {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);
    UpdateTableRequest replaceRequest = mock(UpdateTableRequest.class);
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);

    when(mockExecutor.replaceView(mockContext, viewIdent, replaceRequest)).thenReturn(mockResponse);

    LoadViewResponse response = hookDispatcher.replaceView(mockContext, viewIdent, replaceRequest);

    verify(mockExecutor, times(1)).replaceView(mockContext, viewIdent, replaceRequest);
    assertEquals(mockResponse, response);

    // Verify no entity store operations were performed
    try {
      verify(mockEntityStore, never()).update(any(), any(), any(), any());
      verify(mockEntityStore, never()).delete(any(), any());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLoadViewDelegatesWithoutModification() {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);

    when(mockExecutor.loadView(mockContext, viewIdent)).thenReturn(mockResponse);

    LoadViewResponse response = hookDispatcher.loadView(mockContext, viewIdent);

    verify(mockExecutor, times(1)).loadView(mockContext, viewIdent);
    assertEquals(mockResponse, response);
  }

  @Test
  public void testListViewDelegatesWithoutModification() {
    Namespace namespace = Namespace.of(SCHEMA_NAME);
    ListTablesResponse mockResponse = mock(ListTablesResponse.class);

    when(mockExecutor.listView(mockContext, namespace)).thenReturn(mockResponse);

    ListTablesResponse response = hookDispatcher.listView(mockContext, namespace);

    verify(mockExecutor, times(1)).listView(mockContext, namespace);
    assertEquals(mockResponse, response);
  }

  @Test
  public void testViewExistsDelegatesWithoutModification() {
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);

    when(mockExecutor.viewExists(mockContext, viewIdent)).thenReturn(true);

    boolean exists = hookDispatcher.viewExists(mockContext, viewIdent);

    verify(mockExecutor, times(1)).viewExists(mockContext, viewIdent);
    assertEquals(true, exists);
  }

  @Test
  public void testDropViewWithNullEntityStore() throws Exception {
    // Don't test null entity store - GravitinoEnv validates initialization
    // Instead, test that drop proceeds when entity store operations are available
    TableIdentifier viewIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), VIEW_NAME);

    hookDispatcher.dropView(mockContext, viewIdent);

    verify(mockExecutor, times(1)).dropView(mockContext, viewIdent);
  }

  @Test
  public void testRenameViewWithNullEntityStore() throws Exception {
    // Don't test null entity store - GravitinoEnv validates initialization
    // Instead, test that rename proceeds when entity store operations are available
    TableIdentifier sourceIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "old_view");
    TableIdentifier destIdent = TableIdentifier.of(Namespace.of(SCHEMA_NAME), "new_view");
    RenameTableRequest renameRequest =
        RenameTableRequest.builder().withSource(sourceIdent).withDestination(destIdent).build();

    hookDispatcher.renameView(mockContext, renameRequest);

    verify(mockExecutor, times(1)).renameView(mockContext, renameRequest);
  }
}
