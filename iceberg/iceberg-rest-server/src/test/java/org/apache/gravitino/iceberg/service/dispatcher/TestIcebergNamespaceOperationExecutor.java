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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestIcebergNamespaceOperationExecutor {

  private IcebergNamespaceOperationExecutor executor;
  private IcebergCatalogWrapperManager mockWrapperManager;
  private CatalogWrapperForREST mockCatalogWrapper;
  private IcebergRequestContext mockContext;

  @BeforeEach
  public void setUp() {
    mockWrapperManager = mock(IcebergCatalogWrapperManager.class);
    mockCatalogWrapper = mock(CatalogWrapperForREST.class);
    executor = new IcebergNamespaceOperationExecutor(mockWrapperManager);

    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn("test_catalog");
    when(mockWrapperManager.getCatalogWrapper("test_catalog")).thenReturn(mockCatalogWrapper);
  }

  @Test
  public void testCreateNamespaceOverridesOwnerWithAuthenticatedUser() {
    String authenticatedUser = "user@example.com";
    String clientProvidedOwner = "spark";

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.OWNER, clientProvidedOwner);

    CreateNamespaceRequest originalRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of("test_namespace"))
            .setProperties(properties)
            .build();

    when(mockContext.userName()).thenReturn(authenticatedUser);
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockCatalogWrapper.createNamespace(any())).thenReturn(mockResponse);

    executor.createNamespace(mockContext, originalRequest);

    ArgumentCaptor<CreateNamespaceRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockCatalogWrapper).createNamespace(requestCaptor.capture());

    CreateNamespaceRequest capturedRequest = requestCaptor.getValue();
    String actualOwner = capturedRequest.properties().get(IcebergConstants.OWNER);

    Assertions.assertEquals(authenticatedUser, actualOwner);
    Assertions.assertNotEquals(clientProvidedOwner, actualOwner);
  }

  @Test
  public void testCreateNamespaceAddsOwnerWhenMissing() {
    String authenticatedUser = "user@example.com";

    CreateNamespaceRequest originalRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of("test_namespace")).build();

    when(mockContext.userName()).thenReturn(authenticatedUser);
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockCatalogWrapper.createNamespace(any())).thenReturn(mockResponse);

    executor.createNamespace(mockContext, originalRequest);

    ArgumentCaptor<CreateNamespaceRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockCatalogWrapper).createNamespace(requestCaptor.capture());

    String actualOwner = requestCaptor.getValue().properties().get(IcebergConstants.OWNER);
    Assertions.assertEquals(authenticatedUser, actualOwner);
  }

  @Test
  public void testCreateNamespacePreservesOwnerForAnonymousUser() {
    String clientProvidedOwner = "spark";

    Map<String, String> properties = new HashMap<>();
    properties.put(IcebergConstants.OWNER, clientProvidedOwner);

    CreateNamespaceRequest originalRequest =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of("test_namespace"))
            .setProperties(properties)
            .build();

    when(mockContext.userName()).thenReturn("anonymous");
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockCatalogWrapper.createNamespace(any())).thenReturn(mockResponse);

    executor.createNamespace(mockContext, originalRequest);

    ArgumentCaptor<CreateNamespaceRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockCatalogWrapper).createNamespace(requestCaptor.capture());

    String actualOwner = requestCaptor.getValue().properties().get(IcebergConstants.OWNER);
    Assertions.assertEquals(clientProvidedOwner, actualOwner);
  }
}
