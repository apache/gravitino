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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * The Iceberg REST server must reject a request that sets the reserved {@code gravitino.identifier}
 * property: Gravitino trusts that id on import, so a client that copies another object's id would
 * end up attached to that object's entity, owner and grants.
 */
public class TestIcebergReservedPropertyRejection {

  private static final Namespace NAMESPACE = Namespace.of("test_ns");
  private static final TableIdentifier IDENT = TableIdentifier.of(NAMESPACE, "test_obj");
  private static final Map<String, String> RESERVED_ID_PROPERTIES =
      ImmutableMap.of(StringIdentifier.ID_KEY, StringIdentifier.fromId(42L).toString(), "k", "v");

  private CatalogWrapperForREST catalogWrapper;
  private IcebergRequestContext context;
  private IcebergTableOperationExecutor tableExecutor;
  private IcebergViewOperationExecutor viewExecutor;
  private IcebergNamespaceOperationExecutor namespaceExecutor;

  @BeforeEach
  public void setUp() {
    IcebergCatalogWrapperManager wrapperManager = mock(IcebergCatalogWrapperManager.class);
    catalogWrapper = mock(CatalogWrapperForREST.class);
    when(wrapperManager.getCatalogWrapper("test_catalog")).thenReturn(catalogWrapper);
    context = mock(IcebergRequestContext.class);
    when(context.catalogName()).thenReturn("test_catalog");
    when(context.userName()).thenReturn("user");

    tableExecutor = new IcebergTableOperationExecutor(wrapperManager, Optional.empty());
    viewExecutor = new IcebergViewOperationExecutor(wrapperManager);
    namespaceExecutor = new IcebergNamespaceOperationExecutor(wrapperManager, Optional.empty());
  }

  @Test
  public void testCreateTableRejectsReservedProperty() {
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("test_obj")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
            .setProperties(RESERVED_ID_PROPERTIES)
            .build();

    assertRejected(() -> tableExecutor.createTable(context, NAMESPACE, request));
  }

  @Test
  public void testUpdateTableRejectsReservedProperty() {
    assertRejected(() -> tableExecutor.updateTable(context, IDENT, setPropertiesRequest()));
  }

  @Test
  public void testCreateViewRejectsReservedProperty() {
    CreateViewRequest request = mock(CreateViewRequest.class);
    when(request.properties()).thenReturn(RESERVED_ID_PROPERTIES);

    assertRejected(() -> viewExecutor.createView(context, NAMESPACE, request));
  }

  @Test
  public void testReplaceViewRejectsReservedProperty() {
    assertRejected(() -> viewExecutor.replaceView(context, IDENT, setPropertiesRequest()));
  }

  @Test
  public void testCreateNamespaceRejectsReservedProperty() {
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder()
            .withNamespace(NAMESPACE)
            .setProperties(RESERVED_ID_PROPERTIES)
            .build();

    assertRejected(() -> namespaceExecutor.createNamespace(context, request));
  }

  @Test
  public void testUpdateNamespaceRejectsReservedProperty() {
    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder().updateAll(RESERVED_ID_PROPERTIES).build();

    assertRejected(() -> namespaceExecutor.updateNamespace(context, NAMESPACE, request));
  }

  @Test
  public void testOtherPropertiesAreAccepted() {
    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder()
            .update("k", "v")
            .remove(StringIdentifier.ID_KEY)
            .build();

    namespaceExecutor.updateNamespace(context, NAMESPACE, request);

    verify(catalogWrapper).updateNamespaceProperties(NAMESPACE, request);
  }

  private static UpdateTableRequest setPropertiesRequest() {
    return new UpdateTableRequest(
        ImmutableList.of(),
        ImmutableList.of(new MetadataUpdate.SetProperties(RESERVED_ID_PROPERTIES)));
  }

  private void assertRejected(Executable call) {
    IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, call);
    Assertions.assertTrue(e.getMessage().contains(StringIdentifier.ID_KEY), e.getMessage());
    verifyNoInteractions(catalogWrapper);
  }
}
