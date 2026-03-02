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
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergTableHookDispatcher {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_USER = "test_user";
  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.required(1, "test_field", StringType.get()));

  private IcebergTableHookDispatcher hookDispatcher;
  private IcebergTableOperationDispatcher mockDispatcher;
  private EntityStore mockEntityStore;
  private TableDispatcher mockTableDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private IcebergRequestContext mockContext;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    // Mock the underlying dispatcher
    mockDispatcher = mock(IcebergTableOperationDispatcher.class);

    // Mock GravitinoEnv components
    mockEntityStore = mock(EntityStore.class);
    mockTableDispatcher = mock(TableDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", mockEntityStore, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", mockTableDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    // Create mock IcebergRESTServerContext
    IcebergConfigProvider mockConfigProvider = mock(IcebergConfigProvider.class);
    when(mockConfigProvider.getMetalakeName()).thenReturn(Optional.of(TEST_METALAKE));
    when(mockConfigProvider.getDefaultCatalogName()).thenReturn(TEST_CATALOG);
    IcebergRESTServerContext.create(mockConfigProvider, false, false, null);

    // Create hook dispatcher
    hookDispatcher = new IcebergTableHookDispatcher(mockDispatcher);

    // Mock request context
    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn(TEST_CATALOG);
    when(mockContext.userName()).thenReturn(TEST_USER);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    // Clean up GravitinoEnv
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);

    // Reset IcebergRESTServerContext singleton
    Class<?> holderClass =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("InstanceHolder class not found"));
    FieldUtils.writeStaticField(holderClass, "INSTANCE", null, true);
  }

  @Test
  public void testCreateTableCallsImportAndOwnership() {
    Namespace namespace = Namespace.of("test_schema");
    CreateTableRequest request =
        CreateTableRequest.builder().withName("test_table").withSchema(TABLE_SCHEMA).build();

    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockDispatcher.createTable(mockContext, namespace, request)).thenReturn(mockResponse);

    LoadTableResponse result = hookDispatcher.createTable(mockContext, namespace, request);

    Assertions.assertEquals(mockResponse, result);
    verify(mockDispatcher).createTable(mockContext, namespace, request);

    // Verify table import was called
    NameIdentifier expectedIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            TEST_METALAKE, TEST_CATALOG, TableIdentifier.of(namespace, "test_table"));
    verify(mockTableDispatcher).loadTable(expectedIdentifier);

    // Verify ownership was set
    ArgumentCaptor<String> userCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockOwnerDispatcher)
        .setOwner(eq(TEST_METALAKE), any(), userCaptor.capture(), eq(Owner.Type.USER));
    Assertions.assertEquals(TEST_USER, userCaptor.getValue());
  }

  @Test
  public void testDropTableDeletesEntity() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("test_schema", "test_table");

    hookDispatcher.dropTable(mockContext, tableId, false);

    verify(mockDispatcher).dropTable(mockContext, tableId, false);

    NameIdentifier expectedIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(TEST_METALAKE, TEST_CATALOG, tableId);
    verify(mockEntityStore).delete(expectedIdentifier, Entity.EntityType.TABLE);
  }

  @Test
  public void testDropTableIgnoresNoSuchEntityException() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("test_schema", "test_table");

    NameIdentifier expectedIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(TEST_METALAKE, TEST_CATALOG, tableId);
    doThrow(new NoSuchEntityException("Table not found"))
        .when(mockEntityStore)
        .delete(expectedIdentifier, Entity.EntityType.TABLE);

    // Should not throw exception
    Assertions.assertDoesNotThrow(() -> hookDispatcher.dropTable(mockContext, tableId, false));

    verify(mockDispatcher).dropTable(mockContext, tableId, false);
  }

  @Test
  public void testDropTableThrowsRuntimeExceptionOnIOException() throws IOException {
    TableIdentifier tableId = TableIdentifier.of("test_schema", "test_table");

    doThrow(new IOException("IO error")).when(mockEntityStore).delete(any(), any());

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.dropTable(mockContext, tableId, false));

    Assertions.assertTrue(exception.getMessage().contains("io exception when deleting table"));
    verify(mockDispatcher).dropTable(mockContext, tableId, false);
  }

  @Test
  public void testRenameTableUpdatesEntity() throws IOException {
    TableIdentifier source = TableIdentifier.of("schema1", "old_table");
    TableIdentifier dest = TableIdentifier.of("schema2", "new_table");
    RenameTableRequest request =
        RenameTableRequest.builder().withSource(source).withDestination(dest).build();

    TableEntity mockTableEntity = mock(TableEntity.class);
    when(mockTableEntity.id()).thenReturn(1L);
    when(mockTableEntity.columns()).thenReturn(Collections.emptyList());
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("original_creator").withCreateTime(Instant.now()).build();
    when(mockTableEntity.auditInfo()).thenReturn(auditInfo);

    when(mockEntityStore.update(any(), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any()))
        .thenReturn(mockTableEntity);

    hookDispatcher.renameTable(mockContext, request);

    verify(mockDispatcher).renameTable(mockContext, request);

    NameIdentifier sourceIdentifier =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(TEST_METALAKE, TEST_CATALOG, source);
    verify(mockEntityStore)
        .update(eq(sourceIdentifier), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
  }

  @Test
  public void testRenameTableIgnoresNoSuchEntityException() throws IOException {
    TableIdentifier source = TableIdentifier.of("schema1", "old_table");
    TableIdentifier dest = TableIdentifier.of("schema2", "new_table");
    RenameTableRequest request =
        RenameTableRequest.builder().withSource(source).withDestination(dest).build();

    doThrow(new NoSuchEntityException("Entity not found"))
        .when(mockEntityStore)
        .update(any(), any(), any(), any());

    // Should not throw exception
    Assertions.assertDoesNotThrow(() -> hookDispatcher.renameTable(mockContext, request));

    verify(mockDispatcher).renameTable(mockContext, request);
  }

  @Test
  public void testRenameTableThrowsRuntimeExceptionOnIOException() throws IOException {
    TableIdentifier source = TableIdentifier.of("schema1", "old_table");
    TableIdentifier dest = TableIdentifier.of("schema2", "new_table");
    RenameTableRequest request =
        RenameTableRequest.builder().withSource(source).withDestination(dest).build();

    doThrow(new IOException("IO error")).when(mockEntityStore).update(any(), any(), any(), any());

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.renameTable(mockContext, request));

    Assertions.assertTrue(exception.getMessage().contains("io exception when renaming table"));
    verify(mockDispatcher).renameTable(mockContext, request);
  }

  @Test
  public void testUpdateTablePassesThrough() {
    TableIdentifier tableId = TableIdentifier.of("test_schema", "test_table");
    UpdateTableRequest request = mock(UpdateTableRequest.class);
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);

    when(mockDispatcher.updateTable(mockContext, tableId, request)).thenReturn(mockResponse);

    LoadTableResponse result = hookDispatcher.updateTable(mockContext, tableId, request);

    Assertions.assertEquals(mockResponse, result);
    verify(mockDispatcher).updateTable(mockContext, tableId, request);
  }

  @Test
  public void testLoadTablePassesThrough() {
    TableIdentifier tableId = TableIdentifier.of("test_schema", "test_table");
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);

    when(mockDispatcher.loadTable(mockContext, tableId)).thenReturn(mockResponse);

    LoadTableResponse result = hookDispatcher.loadTable(mockContext, tableId);

    Assertions.assertEquals(mockResponse, result);
    verify(mockDispatcher).loadTable(mockContext, tableId);
  }
}
