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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
<<<<<<< HEAD
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
=======
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
>>>>>>> 8e9ca0009 ([#13565] fix: Validate table and column field lengths (#13551))
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
    executor = new IcebergNamespaceOperationExecutor(mockWrapperManager, Optional.empty());

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

  @Test
  public void testCreateMultiLevelNamespacePassesCorrectNamespace() {
    String authenticatedUser = "user@example.com";
    Namespace multiLevelNs = Namespace.of("team", "sales");

    CreateNamespaceRequest originalRequest =
        CreateNamespaceRequest.builder().withNamespace(multiLevelNs).build();

    when(mockContext.userName()).thenReturn(authenticatedUser);
    CreateNamespaceResponse mockResponse = mock(CreateNamespaceResponse.class);
    when(mockCatalogWrapper.createNamespace(any())).thenReturn(mockResponse);

    executor.createNamespace(mockContext, originalRequest);

    ArgumentCaptor<CreateNamespaceRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    verify(mockCatalogWrapper).createNamespace(requestCaptor.capture());

    CreateNamespaceRequest capturedRequest = requestCaptor.getValue();
    Assertions.assertEquals(multiLevelNs, capturedRequest.namespace());
    Assertions.assertEquals(
        authenticatedUser, capturedRequest.properties().get(IcebergConstants.OWNER));
  }

  @Test
  public void testLoadNamespaceDelegatesToCatalogWrapper() {
    Namespace ns = Namespace.of("A", "B");
    GetNamespaceResponse mockResponse = mock(GetNamespaceResponse.class);
    when(mockCatalogWrapper.loadNamespace(ns)).thenReturn(mockResponse);

    GetNamespaceResponse result = executor.loadNamespace(mockContext, ns);

    verify(mockCatalogWrapper).loadNamespace(ns);
    Assertions.assertEquals(mockResponse, result);
  }

  @Test
<<<<<<< HEAD
=======
  public void testRegisterViewDelegatesToCatalogWrapper() {
    Namespace ns = Namespace.of("test_ns");
    RegisterViewRequest mockRequest = mock(RegisterViewRequest.class);
    LoadViewResponse mockResponse = mock(LoadViewResponse.class);
    when(mockCatalogWrapper.registerView(ns, mockRequest)).thenReturn(mockResponse);

    LoadViewResponse result = executor.registerView(mockContext, ns, mockRequest);

    verify(mockCatalogWrapper).registerView(ns, mockRequest);
    Assertions.assertEquals(mockResponse, result);
  }

  @Test
  public void testRejectsOversizedTableNameBeforeRegister() {
    RegisterTableRequest request = mock(RegisterTableRequest.class);
    when(request.name()).thenReturn("a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executor.registerTable(mockContext, Namespace.of("test_namespace"), request));

    Assertions.assertEquals(
        "The name of the table must not exceed 128 characters", exception.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  @Test
  public void testRejectsOversizedColumnNameBeforeRegister() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
    Schema schema = new Schema(NestedField.required(1, oversizedName, StringType.get()));
    assertRegisterRejectsSchema(schema, "The name of the column must not exceed 128 characters");
  }

  @Test
  public void testRejectsOversizedColumnCommentBeforeRegister() {
    String oversizedComment = "a".repeat(EntityFieldLimits.MAX_COLUMN_COMMENT_LENGTH + 1);
    Schema schema = new Schema(NestedField.required(1, "col1", StringType.get(), oversizedComment));
    assertRegisterRejectsSchema(
        schema, "The comment of the column must not exceed 4096 characters");
  }

  @Test
  public void testInvalidRegisterOverwriteLeavesMetadataUnchanged(@TempDir Path tempDir)
      throws Exception {
    IcebergConfig config =
        new IcebergConfig(
            Map.of(
                IcebergConstants.CATALOG_BACKEND,
                "jdbc",
                IcebergConstants.URI,
                "jdbc:sqlite:" + tempDir.resolve("catalog.db"),
                IcebergConstants.WAREHOUSE,
                tempDir.resolve("warehouse").toString(),
                IcebergConstants.GRAVITINO_JDBC_DRIVER,
                "org.sqlite.JDBC",
                IcebergConstants.ICEBERG_JDBC_USER,
                "test",
                IcebergConstants.ICEBERG_JDBC_PASSWORD,
                "test",
                IcebergConstants.ICEBERG_JDBC_INITIALIZE,
                "true"));
    CatalogWrapperForREST catalogWrapper = new CatalogWrapperForREST("test_catalog", config);
    try {
      when(mockWrapperManager.getCatalogWrapper("test_catalog")).thenReturn(catalogWrapper);
      Namespace namespace = Namespace.of("test_namespace");
      catalogWrapper.createNamespace(
          CreateNamespaceRequest.builder().withNamespace(namespace).build());
      String originalMetadataLocation =
          writeMetadata(
              tempDir.resolve("v1.metadata.json"),
              new Schema(NestedField.required(1, "id", StringType.get())));
      RegisterTableRequest originalRequest =
          ImmutableRegisterTableRequest.builder()
              .name("test_table")
              .metadataLocation(originalMetadataLocation)
              .build();
      catalogWrapper.registerTable(namespace, originalRequest, false);

      String oversizedName = "a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
      String invalidMetadataLocation =
          writeMetadata(
              tempDir.resolve("v2.metadata.json"),
              new Schema(NestedField.required(1, oversizedName, StringType.get())));
      RegisterTableRequest invalidOverwriteRequest =
          ImmutableRegisterTableRequest.builder()
              .name("test_table")
              .metadataLocation(invalidMetadataLocation)
              .overwrite(true)
              .build();

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> executor.registerTable(mockContext, namespace, invalidOverwriteRequest));

      Assertions.assertEquals(
          Optional.of(originalMetadataLocation),
          catalogWrapper.getTableMetadataLocation(
              TableIdentifier.of(namespace, originalRequest.name())));
    } finally {
      catalogWrapper.close();
    }
  }

  @Test
>>>>>>> 8e9ca0009 ([#13565] fix: Validate table and column field lengths (#13551))
  public void testDropNestedNamespacePassesCorrectLevels() {
    Namespace nestedNs = Namespace.of("A", "B", "C");

    executor.dropNamespace(mockContext, nestedNs);

    verify(mockCatalogWrapper).dropNamespace(nestedNs);
  }

  @Test
  public void testDropFlatNamespaceDelegatesToCatalogWrapper() {
    Namespace ns = Namespace.of("mydb");

    executor.dropNamespace(mockContext, ns);

    verify(mockCatalogWrapper).dropNamespace(ns);
  }

  @Test
  public void testListNamespacesWithParentDelegatesToCatalogWrapper() {
    Namespace parent = Namespace.of("A", "B");
    Namespace child = Namespace.of("A", "B", "C");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Collections.singletonList(child)).build();
    when(mockCatalogWrapper.listNamespace(parent)).thenReturn(mockResponse);

    ListNamespacesResponse result = executor.listNamespaces(mockContext, parent);

    verify(mockCatalogWrapper).listNamespace(parent);
    Assertions.assertEquals(mockResponse, result);
    Assertions.assertEquals(Arrays.asList(child), result.namespaces());
  }

  @Test
  public void testListNamespacesAtRootLevelDelegatesToCatalogWrapper() {
    Namespace root = Namespace.empty();
    Namespace ns1 = Namespace.of("A");
    Namespace ns2 = Namespace.of("B");
    ListNamespacesResponse mockResponse =
        ListNamespacesResponse.builder().addAll(Arrays.asList(ns1, ns2)).build();
    when(mockCatalogWrapper.listNamespace(root)).thenReturn(mockResponse);

    ListNamespacesResponse result = executor.listNamespaces(mockContext, root);

    verify(mockCatalogWrapper).listNamespace(root);
    Assertions.assertEquals(2, result.namespaces().size());
  }

  @Test
  public void testNamespaceExistsDelegatesToCatalogWrapper() {
    Namespace nestedNs = Namespace.of("A", "B");
    when(mockCatalogWrapper.namespaceExists(nestedNs)).thenReturn(true);

    boolean exists = executor.namespaceExists(mockContext, nestedNs);

    verify(mockCatalogWrapper).namespaceExists(nestedNs);
    Assertions.assertTrue(exists);
  }

  @Test
  public void testNamespaceNotExistsDelegatesToCatalogWrapper() {
    Namespace ns = Namespace.of("nonexistent");
    when(mockCatalogWrapper.namespaceExists(ns)).thenReturn(false);

    boolean exists = executor.namespaceExists(mockContext, ns);

    verify(mockCatalogWrapper).namespaceExists(ns);
    Assertions.assertFalse(exists);
  }

  private void assertRegisterRejectsSchema(Schema schema, String expectedMessage) {
    Namespace namespace = Namespace.of("test_namespace");
    String metadataLocation = "file:/tmp/test.metadata.json";
    RegisterTableRequest request = mock(RegisterTableRequest.class);
    when(request.name()).thenReturn("test_table");
    when(request.metadataLocation()).thenReturn(metadataLocation);
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), "file:/tmp/table", Collections.emptyMap());
    when(mockCatalogWrapper.loadTableMetadataFromLocation(metadataLocation)).thenReturn(metadata);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executor.registerTable(mockContext, namespace, request));

    Assertions.assertEquals(expectedMessage, exception.getMessage());
    verify(mockCatalogWrapper).loadTableMetadataFromLocation(metadataLocation);
    verify(mockCatalogWrapper, never()).registerTable(namespace, request, false);
  }

  private static String writeMetadata(Path metadataFile, Schema schema) throws Exception {
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            metadataFile.getParent().resolve("table").toUri().toString(),
            Collections.emptyMap());
    Files.writeString(metadataFile, TableMetadataParser.toJson(metadata), StandardCharsets.UTF_8);
    return metadataFile.toUri().toString();
  }
}
