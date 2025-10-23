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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergTableOperationExecutor {

  private static final Schema TABLE_SCHEMA =
      new Schema(NestedField.required(1, "test_field", StringType.get()));

  private IcebergTableOperationExecutor executor;
  private IcebergCatalogWrapperManager mockWrapperManager;
  private CatalogWrapperForREST mockCatalogWrapper;
  private IcebergRequestContext mockContext;

  @BeforeEach
  public void setUp() {
    mockWrapperManager = mock(IcebergCatalogWrapperManager.class);
    mockCatalogWrapper = mock(CatalogWrapperForREST.class);
    executor = new IcebergTableOperationExecutor(mockWrapperManager);

    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn("test_catalog");
    when(mockContext.requestCredentialVending()).thenReturn(false);
    when(mockWrapperManager.getCatalogWrapper("test_catalog")).thenReturn(mockCatalogWrapper);
  }

  @Test
  public void testCreateTableOverridesOwnerWithAuthenticatedUser() {
    String authenticatedUser = "user@example.com";
    String clientProvidedOwner = "spark";

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.OWNER, clientProvidedOwner);

    CreateTableRequest originalRequest =
        CreateTableRequest.builder()
            .withName("test_table")
            .withSchema(TABLE_SCHEMA)
            .setProperties(properties)
            .build();

    when(mockContext.userName()).thenReturn(authenticatedUser);
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockCatalogWrapper.createTable(any(), any(), anyBoolean())).thenReturn(mockResponse);

    executor.createTable(mockContext, Namespace.of("test_namespace"), originalRequest);

    ArgumentCaptor<CreateTableRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockCatalogWrapper).createTable(any(), requestCaptor.capture(), anyBoolean());

    CreateTableRequest capturedRequest = requestCaptor.getValue();
    String actualOwner = capturedRequest.properties().get(IcebergConstants.OWNER);

    Assertions.assertEquals(authenticatedUser, actualOwner);
    Assertions.assertNotEquals(clientProvidedOwner, actualOwner);
  }

  @Test
  public void testCreateTableAddsOwnerWhenMissing() {
    String authenticatedUser = "user@example.com";

    CreateTableRequest originalRequest =
        CreateTableRequest.builder().withName("test_table").withSchema(TABLE_SCHEMA).build();

    when(mockContext.userName()).thenReturn(authenticatedUser);
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockCatalogWrapper.createTable(any(), any(), anyBoolean())).thenReturn(mockResponse);

    executor.createTable(mockContext, Namespace.of("test_namespace"), originalRequest);

    ArgumentCaptor<CreateTableRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockCatalogWrapper).createTable(any(), requestCaptor.capture(), anyBoolean());

    String actualOwner = requestCaptor.getValue().properties().get(IcebergConstants.OWNER);
    Assertions.assertEquals(authenticatedUser, actualOwner);
  }

  @Test
  public void testCreateTablePreservesOwnerForAnonymousUser() {
    String clientProvidedOwner = "spark";

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.OWNER, clientProvidedOwner);

    CreateTableRequest originalRequest =
        CreateTableRequest.builder()
            .withName("test_table")
            .withSchema(TABLE_SCHEMA)
            .setProperties(properties)
            .build();

    when(mockContext.userName()).thenReturn("anonymous");
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockCatalogWrapper.createTable(any(), any(), anyBoolean())).thenReturn(mockResponse);

    executor.createTable(mockContext, Namespace.of("test_namespace"), originalRequest);

    ArgumentCaptor<CreateTableRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockCatalogWrapper).createTable(any(), requestCaptor.capture(), anyBoolean());

    String actualOwner = requestCaptor.getValue().properties().get(IcebergConstants.OWNER);
    Assertions.assertEquals(clientProvidedOwner, actualOwner);
  }
}
