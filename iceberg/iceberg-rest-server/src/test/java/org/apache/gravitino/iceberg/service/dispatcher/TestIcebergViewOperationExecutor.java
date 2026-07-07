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
import static org.mockito.Mockito.when;

import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergViewOperationExecutor {

  private IcebergViewOperationExecutor executor;
  private IcebergCatalogWrapperManager mockWrapperManager;
  private CatalogWrapperForREST mockCatalogWrapper;
  private IcebergRequestContext mockContext;

  @BeforeEach
  public void setUp() {
    mockWrapperManager = mock(IcebergCatalogWrapperManager.class);
    mockCatalogWrapper = mock(CatalogWrapperForREST.class);
    executor = new IcebergViewOperationExecutor(mockWrapperManager);

    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn("test_catalog");
    when(mockWrapperManager.getCatalogWrapper("test_catalog")).thenReturn(mockCatalogWrapper);
  }

  @Test
  public void testCreateView() {
    Namespace namespace = Namespace.of("test_ns");
    CreateViewRequest mockRequest = mock(CreateViewRequest.class);
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockCatalogWrapper.createView(namespace, mockRequest)).thenReturn(mockResponse);

    LoadViewResponse result = executor.createView(mockContext, namespace, mockRequest);

    Assertions.assertEquals(mockResponse, result);
    verify(mockCatalogWrapper).createView(namespace, mockRequest);
  }

  @Test
  public void testLoadView() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "test_view");
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockCatalogWrapper.loadView(viewId)).thenReturn(mockResponse);

    LoadViewResponse result = executor.loadView(mockContext, viewId);

    Assertions.assertEquals(mockResponse, result);
    verify(mockCatalogWrapper).loadView(viewId);
  }

  @Test
  public void testDropView() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "test_view");

    executor.dropView(mockContext, viewId);

    verify(mockCatalogWrapper).dropView(viewId);
  }

  @Test
  public void testListView() {
    Namespace namespace = Namespace.of("test_ns");
    ListTablesResponse mockResponse = mock(ListTablesResponse.class);
    when(mockCatalogWrapper.listView(namespace)).thenReturn(mockResponse);

    ListTablesResponse result = executor.listView(mockContext, namespace);

    Assertions.assertEquals(mockResponse, result);
    verify(mockCatalogWrapper).listView(namespace);
  }

  @Test
  public void testViewExists() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "test_view");
    when(mockCatalogWrapper.viewExists(viewId)).thenReturn(true);

    boolean result = executor.viewExists(mockContext, viewId);

    Assertions.assertTrue(result);
    verify(mockCatalogWrapper).viewExists(viewId);
  }

  @Test
  public void testViewDoesNotExist() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "non_existent_view");
    when(mockCatalogWrapper.viewExists(viewId)).thenReturn(false);

    boolean result = executor.viewExists(mockContext, viewId);

    Assertions.assertFalse(result);
    verify(mockCatalogWrapper).viewExists(viewId);
  }

  @Test
  public void testLoadViewThrowsException() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "test_view");
    RuntimeException exception = new RuntimeException("View not found");
    when(mockCatalogWrapper.loadView(viewId)).thenThrow(exception);

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> executor.loadView(mockContext, viewId));

    Assertions.assertEquals(exception, thrown);
    verify(mockCatalogWrapper).loadView(viewId);
  }

  @Test
  public void testCreateViewThrowsException() {
    Namespace namespace = Namespace.of("test_ns");
    CreateViewRequest mockRequest = mock(CreateViewRequest.class);
    RuntimeException exception = new RuntimeException("Failed to create view");
    when(mockCatalogWrapper.createView(namespace, mockRequest)).thenThrow(exception);

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class, () -> executor.createView(mockContext, namespace, mockRequest));

    Assertions.assertEquals(exception, thrown);
    verify(mockCatalogWrapper).createView(namespace, mockRequest);
  }

  @Test
  public void testReplaceView() {
    TableIdentifier viewId = TableIdentifier.of("test_ns", "test_view");
    UpdateTableRequest mockRequest = mock(UpdateTableRequest.class);
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockCatalogWrapper.updateView(viewId, mockRequest)).thenReturn(mockResponse);

    LoadViewResponse result = executor.replaceView(mockContext, viewId, mockRequest);

    Assertions.assertEquals(mockResponse, result);
    verify(mockCatalogWrapper).updateView(viewId, mockRequest);
  }

  @Test
  public void testRenameView() {
    RenameTableRequest mockRequest = mock(RenameTableRequest.class);

    executor.renameView(mockContext, mockRequest);

    verify(mockCatalogWrapper).renameView(mockRequest);
  }
}
