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
  private EntityStore mockEntityStore;
  private LockManager mockLockManager;
  private IcebergRequestContext mockContext;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(IcebergNamespaceOperationDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    SchemaDispatcher mockSchemaDispatcher = mock(SchemaDispatcher.class);
    TableDispatcher mockTableDispatcher = mock(TableDispatcher.class);

    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "schemaDispatcher", mockSchemaDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", mockTableDispatcher, true);

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
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", null, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", null, true);

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
        .when(mockOwnerDispatcher)
        .setOwners(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.createNamespace(mockContext, mockRequest));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).createNamespace(mockContext, mockRequest);
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
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.registerTable(mockContext, namespace, mockRequest));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).registerTable(mockContext, namespace, mockRequest);
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
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    doThrow(new RuntimeException("Import failed")).when(schemaDispatcher).loadSchema(any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> hookDispatcher.createNamespace(mockContext, mockRequest));

    Assertions.assertEquals("Import failed", thrown.getMessage());
    verify(mockOwnerDispatcher, never()).setOwners(any(), any(), any(), any());
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
    TableDispatcher tableDispatcher = GravitinoEnv.getInstance().tableDispatcher();
    doThrow(new RuntimeException("Import failed")).when(tableDispatcher).loadTable(any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.registerTable(mockContext, namespace, mockRequest));

    Assertions.assertEquals("Import failed", thrown.getMessage());
    verify(mockOwnerDispatcher, never()).setOwner(any(), any(), any(), any());
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
  public void testCreateNestedNamespaceOwnsOnlyMissingAncestorsAndLeaf() {
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
    verify(mockOwnerDispatcher)
        .setOwners(eq(TEST_METALAKE), captor.capture(), eq(TEST_USER), eq(Owner.Type.USER));
    List<String> names =
        captor.getValue().stream().map(MetadataObject::fullName).collect(Collectors.toList());
    Assertions.assertEquals(
        Arrays.asList(TEST_CATALOG + ".A:B", TEST_CATALOG + ".A:B:C", TEST_CATALOG + ".A:B:C:D"),
        names);
  }

  @Test
  public void testDropNamespaceCascadeDeletesOutermostPhantomAncestor() throws Exception {
    // Drop A:B:C. Neither A nor A:B exists in Iceberg, so the whole branch is Gravitino-side
    // phantoms. The hook cascade-deletes the outermost phantom (A) so the future hierarchical
    // cascade in the entity store can clean up the whole sub-tree in one call.
    Namespace leaf = Namespace.of("A", "B", "C");
    Namespace parent = Namespace.of("A", "B");
    Namespace grandparent = Namespace.of("A");

    hookDispatcher.dropNamespace(mockContext, leaf);

    verify(mockDispatcher).dropNamespace(mockContext, leaf);
    verify(mockDispatcher, never()).dropNamespace(mockContext, parent);
    verify(mockDispatcher, never()).dropNamespace(mockContext, grandparent);

    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(mockEntityStore, org.mockito.Mockito.times(1))
        .delete(captor.capture(), eq(Entity.EntityType.SCHEMA), eq(true));
    Assertions.assertEquals("A", captor.getValue().name());
  }

  @Test
  public void testDropNamespaceCascadeDeletesTargetWhenAncestorsExist() throws Exception {
    // Drop A:B:C; A:B still exists in Iceberg. By the child-exists-implies-parent-exists
    // invariant A is also live, so there is no phantom to cascade-delete from. The hook
    // cascade-deletes just the target's row.
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

    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(mockEntityStore, org.mockito.Mockito.times(1))
        .delete(captor.capture(), eq(Entity.EntityType.SCHEMA), eq(true));
    Assertions.assertEquals("A:B:C", captor.getValue().name());
  }
}
