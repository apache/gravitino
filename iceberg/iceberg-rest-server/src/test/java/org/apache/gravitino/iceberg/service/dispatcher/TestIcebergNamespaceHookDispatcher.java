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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.lock.TreeLock;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergNamespaceHookDispatcher {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_USER = "test_user";

  private IcebergNamespaceHookDispatcher hookDispatcher;
  private IcebergNamespaceOperationDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  private OwnerDispatcher mockInternalOwnerDispatcher;
  private SchemaDispatcher mockSchemaDispatcher;
  private SchemaDispatcher mockInternalSchemaDispatcher;
  private TableDispatcher mockTableDispatcher;
  private TableDispatcher mockInternalTableDispatcher;
  private EntityStore mockEntityStore;
  private LockManager mockLockManager;
  private IcebergRequestContext mockContext;

  private OwnerDispatcher previousOwnerDispatcher;
  private OwnerDispatcher previousInternalOwnerDispatcher;
  private SchemaDispatcher previousSchemaDispatcher;
  private SchemaDispatcher previousInternalSchemaDispatcher;
  private TableDispatcher previousTableDispatcher;
  private TableDispatcher previousInternalTableDispatcher;
  private EntityStore previousEntityStore;
  private LockManager previousLockManager;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(IcebergNamespaceOperationDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    mockInternalOwnerDispatcher = mock(OwnerDispatcher.class);
    mockSchemaDispatcher = mock(SchemaDispatcher.class);
    mockInternalSchemaDispatcher = mock(SchemaDispatcher.class);
    mockTableDispatcher = mock(TableDispatcher.class);
    mockInternalTableDispatcher = mock(TableDispatcher.class);

    previousOwnerDispatcher =
        (OwnerDispatcher) FieldUtils.readField(GravitinoEnv.getInstance(), "ownerDispatcher", true);
    previousInternalOwnerDispatcher =
        (OwnerDispatcher)
            FieldUtils.readField(GravitinoEnv.getInstance(), "internalOwnerDispatcher", true);
    previousSchemaDispatcher =
        (SchemaDispatcher)
            FieldUtils.readField(GravitinoEnv.getInstance(), "schemaDispatcher", true);
    previousInternalSchemaDispatcher =
        (SchemaDispatcher)
            FieldUtils.readField(GravitinoEnv.getInstance(), "internalSchemaDispatcher", true);
    previousTableDispatcher =
        (TableDispatcher) FieldUtils.readField(GravitinoEnv.getInstance(), "tableDispatcher", true);
    previousInternalTableDispatcher =
        (TableDispatcher)
            FieldUtils.readField(GravitinoEnv.getInstance(), "internalTableDispatcher", true);
    previousEntityStore =
        (EntityStore) FieldUtils.readField(GravitinoEnv.getInstance(), "entityStore", true);
    previousLockManager =
        (LockManager) FieldUtils.readField(GravitinoEnv.getInstance(), "lockManager", true);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalOwnerDispatcher", mockInternalOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", mockSchemaDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalSchemaDispatcher", mockInternalSchemaDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", mockTableDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalTableDispatcher", mockInternalTableDispatcher, true);

    mockEntityStore = mock(EntityStore.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", mockEntityStore, true);

    mockLockManager = mock(LockManager.class);
    TreeLock mockTreeLock = mock(TreeLock.class);
    when(mockLockManager.createTreeLock(any())).thenReturn(mockTreeLock);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", mockLockManager, true);

    IcebergConfigProvider mockConfigProvider = mock(IcebergConfigProvider.class);
    when(mockConfigProvider.getMetalakeName()).thenReturn(TEST_METALAKE);
    when(mockConfigProvider.getDefaultCatalogName()).thenReturn(TEST_CATALOG);
    IcebergRESTServerContext.create(mockConfigProvider, false, false, true, null);

    hookDispatcher = new IcebergNamespaceHookDispatcher(mockDispatcher);

    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn(TEST_CATALOG);
    when(mockContext.userName()).thenReturn(TEST_USER);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "ownerDispatcher", previousOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalOwnerDispatcher",
        previousInternalOwnerDispatcher,
        true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", previousSchemaDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalSchemaDispatcher",
        previousInternalSchemaDispatcher,
        true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "tableDispatcher", previousTableDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalTableDispatcher",
        previousInternalTableDispatcher,
        true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", previousEntityStore, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", previousLockManager, true);

    Class<?> holderClass =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("InstanceHolder class not found"));
    FieldUtils.writeStaticField(holderClass, "INSTANCE", null, true);
  }

  @Test
  public void testCreateNamespaceThrowsWhenSetOwnerFails() {
    Namespace namespace = Namespace.of("test_schema");
    CreateNamespaceRequest mockRequest = mock(CreateNamespaceRequest.class);
    when(mockRequest.namespace()).thenReturn(namespace);

    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockDispatcher.createNamespace(mockContext, mockRequest)).thenReturn(mockResponse);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockInternalOwnerDispatcher)
        .setOwners(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.createNamespace(mockContext, mockRequest));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).createNamespace(mockContext, mockRequest);
    verify(mockOwnerDispatcher, never()).setOwners(any(), any(), any(), any());
  }

  @Test
  public void testRegisterTableThrowsWhenSetOwnerFails() {
    Namespace namespace = Namespace.of("test_schema");
    RegisterTableRequest mockRequest = mock(RegisterTableRequest.class);
    when(mockRequest.name()).thenReturn("test_table");

    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockDispatcher.registerTable(mockContext, namespace, mockRequest))
        .thenReturn(mockResponse);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockInternalOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.registerTable(mockContext, namespace, mockRequest));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).registerTable(mockContext, namespace, mockRequest);
    verify(mockOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
  }

  @Test
  public void testCreateNamespacePropagatesImportFailure() {
    Namespace namespace = Namespace.of("test_schema");
    CreateNamespaceRequest mockRequest = mock(CreateNamespaceRequest.class);
    when(mockRequest.namespace()).thenReturn(namespace);

    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockDispatcher.createNamespace(mockContext, mockRequest)).thenReturn(mockResponse);

    // Schema import (loadSchema) throwing must propagate so the caller learns the namespace
    // exists in Iceberg but is not registered in Gravitino. setOwner is therefore unreachable.
    doThrow(new RuntimeException("Import failed"))
        .when(mockInternalSchemaDispatcher)
        .loadSchema(any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.createNamespace(mockContext, mockRequest));

    Assertions.assertEquals("Import failed", thrown.getMessage());
    verify(mockInternalOwnerDispatcher, never()).setOwners(any(), any(), any(), any());
    verify(mockOwnerDispatcher, never()).setOwners(any(), any(), any(), any());
    verify(mockSchemaDispatcher, never()).loadSchema(any());
  }

  @Test
  public void testRegisterTablePropagatesImportFailure() {
    Namespace namespace = Namespace.of("test_schema");
    RegisterTableRequest mockRequest = mock(RegisterTableRequest.class);
    when(mockRequest.name()).thenReturn("test_table");

    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockDispatcher.registerTable(mockContext, namespace, mockRequest))
        .thenReturn(mockResponse);

    // Table import (loadTable) throwing must propagate so the caller learns the table exists in
    // Iceberg but is not registered in Gravitino. setOwner is therefore unreachable.
    doThrow(new RuntimeException("Import failed"))
        .when(mockInternalTableDispatcher)
        .loadTable(any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.registerTable(mockContext, namespace, mockRequest));

    Assertions.assertEquals("Import failed", thrown.getMessage());
    verify(mockInternalOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
    verify(mockOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
    verify(mockTableDispatcher, never()).loadTable(any());
  }

  @Test
  public void testCreateNamespaceLocksTopLevelBranchRoot() {
    // Lock target must be the top-level branch root (metalake.catalog.A), not the catalog —
    // that's what lets disjoint top-level branches create in parallel.
    Namespace leaf = Namespace.of("A", "B", "C");
    CreateNamespaceRequest request = mock(CreateNamespaceRequest.class);
    when(request.namespace()).thenReturn(leaf);
    when(mockDispatcher.createNamespace(mockContext, request))
        .thenReturn(mock(CreateNamespaceResponse.class));

    hookDispatcher.createNamespace(mockContext, request);

    ArgumentCaptor<NameIdentifier> lockId = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(mockLockManager).createTreeLock(lockId.capture());
    Assertions.assertArrayEquals(
        new String[] {TEST_METALAKE, TEST_CATALOG}, lockId.getValue().namespace().levels());
    Assertions.assertEquals("A", lockId.getValue().name());
  }

  @Test
  public void testCreateNestedNamespaceOwnsMissingAncestorsAndLeaf() {
    // Depth-4 leaf, only A exists. Expect setOwners with [A:B, A:B:C, A:B:C:D] —
    // missing ancestors (outermost first) + leaf; existing A is not re-owned.
    Namespace leaf = Namespace.of("A", "B", "C", "D");
    Set<String> existing = ImmutableSet.of("A");
    when(mockDispatcher.namespaceExists(eq(mockContext), any(Namespace.class)))
        .thenAnswer(
            inv -> {
              Namespace ns = inv.getArgument(1, Namespace.class);
              return existing.contains(String.join(":", ns.levels()));
            });

    CreateNamespaceRequest request = mock(CreateNamespaceRequest.class);
    when(request.namespace()).thenReturn(leaf);
    when(mockDispatcher.createNamespace(mockContext, request))
        .thenReturn(mock(CreateNamespaceResponse.class));

    hookDispatcher.createNamespace(mockContext, request);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataObject>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockInternalOwnerDispatcher)
        .setOwners(eq(TEST_METALAKE), captor.capture(), eq(TEST_USER), eq(Owner.Type.USER));
    verify(mockOwnerDispatcher, never()).setOwners(any(), any(), any(), any());
    List<String> names =
        captor.getValue().stream().map(MetadataObject::fullName).collect(Collectors.toList());
    Assertions.assertEquals(
        Arrays.asList(TEST_CATALOG + ".A:B", TEST_CATALOG + ".A:B:C", TEST_CATALOG + ".A:B:C:D"),
        names);
  }

  @Test
  public void testDropNamespaceDeletesTargetAndPhantomAncestors() throws Exception {
    Namespace leaf = Namespace.of("A", "B", "C");
    Namespace parent = Namespace.of("A", "B");
    Namespace grandparent = Namespace.of("A");

    hookDispatcher.dropNamespace(mockContext, leaf);

    verify(mockDispatcher).dropNamespace(mockContext, leaf);
    verify(mockDispatcher, never()).dropNamespace(mockContext, parent);
    verify(mockDispatcher, never()).dropNamespace(mockContext, grandparent);

    // The leaf and both phantom ancestors are stale, so a single cascade delete of the outermost
    // empty ancestor (A) removes the whole stale chain in one batched operation.
    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(mockEntityStore, times(1))
        .delete(captor.capture(), eq(Entity.EntityType.SCHEMA), eq(true));
    Assertions.assertEquals("A", captor.getValue().name());
  }

  @Test
  public void testDropNamespaceDeletesTargetWhenAncestorsExist() throws Exception {
    Namespace leaf = Namespace.of("A", "B", "C");
    Namespace parent = Namespace.of("A", "B");
    Namespace grandparent = Namespace.of("A");

    when(mockDispatcher.namespaceExists(mockContext, parent)).thenReturn(true);

    hookDispatcher.dropNamespace(mockContext, leaf);

    verify(mockDispatcher).dropNamespace(mockContext, leaf);
    verify(mockDispatcher, never()).dropNamespace(mockContext, parent);
    verify(mockDispatcher, never()).dropNamespace(mockContext, grandparent);
    verify(mockDispatcher).namespaceExists(mockContext, parent);
    verify(mockDispatcher, never()).namespaceExists(mockContext, grandparent);

    // The parent still exists, so only the leaf is stale; it is cascade-deleted on its own.
    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(mockEntityStore, times(1))
        .delete(captor.capture(), eq(Entity.EntityType.SCHEMA), eq(true));
    Assertions.assertEquals("A:B:C", captor.getValue().name());
  }
}
