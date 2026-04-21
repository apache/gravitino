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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergNamespaceHookDispatcher {

  private static final String TEST_METALAKE = "test_metalake";
  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_USER = "test_user";

  private IcebergNamespaceHookDispatcher hookDispatcher;
  private IcebergNamespaceOperationDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
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

    Class<?> holderClass =
        Arrays.stream(IcebergRESTServerContext.class.getDeclaredClasses())
            .filter(c -> c.getSimpleName().equals("InstanceHolder"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("InstanceHolder class not found"));
    FieldUtils.writeStaticField(holderClass, "INSTANCE", null, true);
  }

  @Test
  public void testCreateNamespaceSucceedsEvenIfSetOwnerFails() {
    Namespace namespace = Namespace.of("test_schema");
    CreateNamespaceRequest mockRequest = mock(CreateNamespaceRequest.class);
    when(mockRequest.namespace()).thenReturn(namespace);

    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockDispatcher.createNamespace(mockContext, mockRequest)).thenReturn(mockResponse);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    CreateNamespaceResponse result = hookDispatcher.createNamespace(mockContext, mockRequest);

    Assertions.assertEquals(mockResponse, result);
    verify(mockDispatcher).createNamespace(mockContext, mockRequest);
  }

  @Test
  public void testRegisterTableSucceedsEvenIfSetOwnerFails() {
    Namespace namespace = Namespace.of("test_schema");
    RegisterTableRequest mockRequest = mock(RegisterTableRequest.class);
    when(mockRequest.name()).thenReturn("test_table");

    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockDispatcher.registerTable(mockContext, namespace, mockRequest))
        .thenReturn(mockResponse);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    LoadTableResponse result = hookDispatcher.registerTable(mockContext, namespace, mockRequest);

    Assertions.assertEquals(mockResponse, result);
    verify(mockDispatcher).registerTable(mockContext, namespace, mockRequest);
  }
}
