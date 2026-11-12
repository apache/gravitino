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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

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
    executor = new IcebergTableOperationExecutor(mockWrapperManager, Optional.empty());

    mockContext = mock(IcebergRequestContext.class);
    when(mockContext.catalogName()).thenReturn("test_catalog");
    when(mockContext.requestCredentialVending()).thenReturn(false);
    when(mockWrapperManager.getCatalogWrapper("test_catalog")).thenReturn(mockCatalogWrapper);
  }

  @Test
  void testVendedCredentialsFollowReadAndWritePermissions() {
    configureCredentialVending();
    TableIdentifier table = TableIdentifier.of("schema", "table");
    for (CredentialPrivilege privilege : CredentialPrivilege.values()) {
      try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class)) {
        helper
            .when(
                () ->
                    MetadataAuthzHelper.checkAccess(
                        any(),
                        eq(Entity.EntityType.TABLE),
                        eq(
                            AuthorizationExpressionConstants
                                .FILTER_WRITE_TABLE_AUTHORIZATION_EXPRESSION)))
            .thenReturn(privilege == CredentialPrivilege.WRITE);
        helper
            .when(
                () ->
                    MetadataAuthzHelper.checkAccess(
                        any(),
                        eq(Entity.EntityType.TABLE),
                        eq(
                            AuthorizationExpressionConstants
                                .FILTER_READ_TABLE_AUTHORIZATION_EXPRESSION)))
            .thenReturn(privilege == CredentialPrivilege.READ);
        executor.loadTable(mockContext, table);
        verify(mockCatalogWrapper).loadTable(table, true, privilege);
        executor.getTableCredentials(mockContext, table);
        verify(mockCatalogWrapper).getTableCredentials(table, privilege);
      }
    }
  }

  @Test
  void testMetadataOnlyAccessDoesNotVendCredentials() {
    configureCredentialVending();
    TableIdentifier table = TableIdentifier.of("schema", "table");
    try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class)) {
      executor.loadTable(mockContext, table);
      verify(mockCatalogWrapper).loadTable(table, false, CredentialPrivilege.READ);
      Assertions.assertThrows(
          ForbiddenException.class, () -> executor.getTableCredentials(mockContext, table));
    }
  }

  private void configureCredentialVending() {
    IcebergConfigProvider config = mock(IcebergConfigProvider.class);
    when(config.getMetalakeName()).thenReturn("metalake");
    when(config.getDefaultCatalogName()).thenReturn("test_catalog");
    IcebergRESTServerContext.create(config, true, false, false, mockWrapperManager);
    when(mockContext.requestCredentialVending()).thenReturn(true);
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

  @Test
  public void testCreateTablePreservesStageCreateTrue() {
    when(mockContext.userName()).thenReturn("user@example.com");
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockCatalogWrapper.createTable(any(), any(), anyBoolean())).thenReturn(mockResponse);

    CreateTableRequest stagedRequest =
        CreateTableRequest.builder()
            .withName("test_table")
            .withSchema(TABLE_SCHEMA)
            .stageCreate()
            .build();

    executor.createTable(mockContext, Namespace.of("test_namespace"), stagedRequest);

    ArgumentCaptor<CreateTableRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockCatalogWrapper).createTable(any(), requestCaptor.capture(), anyBoolean());

    Assertions.assertTrue(
        requestCaptor.getValue().stageCreate(),
        "stageCreate=true must be preserved when rebuilding request");
  }

  @Test
  public void testCreateTablePreservesStageCreateFalse() {
    when(mockContext.userName()).thenReturn("user@example.com");
    LoadTableResponse mockResponse = mock(LoadTableResponse.class);
    when(mockCatalogWrapper.createTable(any(), any(), anyBoolean())).thenReturn(mockResponse);

    CreateTableRequest normalRequest =
        CreateTableRequest.builder().withName("test_table").withSchema(TABLE_SCHEMA).build();

    executor.createTable(mockContext, Namespace.of("test_namespace"), normalRequest);

    ArgumentCaptor<CreateTableRequest> requestCaptor =
        ArgumentCaptor.forClass(CreateTableRequest.class);
    verify(mockCatalogWrapper).createTable(any(), requestCaptor.capture(), anyBoolean());

    Assertions.assertFalse(
        requestCaptor.getValue().stageCreate(),
        "stageCreate=false must remain false when rebuilding request");
  }

  @Test
  public void testRejectsOversizedTableNameBeforeCreate() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
    CreateTableRequest request =
        CreateTableRequest.builder().withName(oversizedName).withSchema(TABLE_SCHEMA).build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executor.createTable(mockContext, Namespace.of("test_namespace"), request));

    Assertions.assertEquals(
        "The name of the table must not exceed 128 characters", exception.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  @Test
  public void testRejectsOversizedTableNameBeforeRename() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
    RenameTableRequest request =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of("test_namespace", "source"))
            .withDestination(TableIdentifier.of("test_namespace", oversizedName))
            .build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> executor.renameTable(mockContext, request));

    Assertions.assertEquals(
        "The name of the table must not exceed 128 characters", exception.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  @ParameterizedTest
  @ValueSource(
      ints = {EntityFieldLimits.MAX_NAME_LENGTH + 1, EntityFieldLimits.MAX_COLUMN_NAME_LENGTH})
  void testExpandedColumnNamesReachCreateAndUpdate(int length) {
    Schema schema = new Schema(NestedField.required(1, "a".repeat(length), StringType.get()));
    Namespace namespace = Namespace.of("test_namespace");
    CreateTableRequest createRequest =
        CreateTableRequest.builder().withName("test_table").withSchema(schema).build();
    when(mockContext.userName()).thenReturn(AuthConstants.ANONYMOUS_USER);

    executor.createTable(mockContext, namespace, createRequest);

    verify(mockCatalogWrapper).createTable(namespace, createRequest, false);

    TableIdentifier table = TableIdentifier.of(namespace, "test_table");
    UpdateTableRequest updateRequest = updateRequest(schema);

    executor.updateTable(mockContext, table, updateRequest);

    verify(mockCatalogWrapper).updateTable(table, updateRequest);
  }

  @Test
  public void testRejectsOversizedColumnFieldsBeforeCreate() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_COLUMN_NAME_LENGTH + 1);
    Schema oversizedNameSchema =
        new Schema(NestedField.required(1, oversizedName, StringType.get()));
    CreateTableRequest oversizedNameRequest =
        CreateTableRequest.builder().withName("test_table").withSchema(oversizedNameSchema).build();

    IllegalArgumentException nameException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.createTable(
                    mockContext, Namespace.of("test_namespace"), oversizedNameRequest));
    Assertions.assertEquals(
        "The name of the column must not exceed "
            + EntityFieldLimits.MAX_COLUMN_NAME_LENGTH
            + " characters",
        nameException.getMessage());

    String oversizedComment = "a".repeat(EntityFieldLimits.MAX_COLUMN_COMMENT_LENGTH + 1);
    Schema oversizedCommentSchema =
        new Schema(NestedField.required(1, "col1", StringType.get(), oversizedComment));
    CreateTableRequest oversizedCommentRequest =
        CreateTableRequest.builder()
            .withName("test_table")
            .withSchema(oversizedCommentSchema)
            .build();

    IllegalArgumentException commentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.createTable(
                    mockContext, Namespace.of("test_namespace"), oversizedCommentRequest));
    Assertions.assertEquals(
        "The comment of the column must not exceed 4096 characters", commentException.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  @Test
  public void testRejectsUnrepresentableDecimalBeforeCreate() {
    // Iceberg accepts decimal(10, 11); Gravitino cannot import it, so it must be refused before the
    // table is created rather than after.
    Schema schema =
        new Schema(
            NestedField.required(1, "id", StringType.get()),
            NestedField.optional(
                2,
                "s",
                Types.StructType.of(
                    NestedField.optional(3, "amount", Types.DecimalType.of(10, 11)))));
    CreateTableRequest request =
        CreateTableRequest.builder().withName("test_table").withSchema(schema).build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> executor.createTable(mockContext, Namespace.of("test_namespace"), request));
    Assertions.assertTrue(exception.getMessage().contains("amount"), exception.getMessage());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            executor.updateTable(
                mockContext,
                TableIdentifier.of("test_namespace", "test_table"),
                updateRequest(schema)));
    verifyNoInteractions(mockCatalogWrapper);
  }

  @Test
  public void testRejectsOversizedColumnFieldsBeforeUpdate() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_COLUMN_NAME_LENGTH + 1);
    UpdateTableRequest oversizedNameRequest =
        updateRequest(new Schema(NestedField.required(1, oversizedName, StringType.get())));

    IllegalArgumentException nameException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.updateTable(
                    mockContext,
                    TableIdentifier.of("test_namespace", "test_table"),
                    oversizedNameRequest));
    Assertions.assertEquals(
        "The name of the column must not exceed "
            + EntityFieldLimits.MAX_COLUMN_NAME_LENGTH
            + " characters",
        nameException.getMessage());

    String oversizedComment = "a".repeat(EntityFieldLimits.MAX_COLUMN_COMMENT_LENGTH + 1);
    UpdateTableRequest oversizedCommentRequest =
        updateRequest(
            new Schema(NestedField.required(1, "col1", StringType.get(), oversizedComment)));

    IllegalArgumentException commentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.updateTable(
                    mockContext,
                    TableIdentifier.of("test_namespace", "test_table"),
                    oversizedCommentRequest));
    Assertions.assertEquals(
        "The comment of the column must not exceed 4096 characters", commentException.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  @Test
  public void testRejectsOversizedTableNameBeforeStagedCreateCommit() {
    String oversizedName = "a".repeat(EntityFieldLimits.MAX_NAME_LENGTH + 1);
    UpdateTableRequest request =
        new UpdateTableRequest(
            Collections.singletonList(new UpdateRequirement.AssertTableDoesNotExist()),
            Collections.emptyList());

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                executor.updateTable(
                    mockContext, TableIdentifier.of("test_namespace", oversizedName), request));

    Assertions.assertEquals(
        "The name of the table must not exceed 128 characters", exception.getMessage());
    verifyNoInteractions(mockCatalogWrapper);
  }

  private static UpdateTableRequest updateRequest(Schema schema) {
    return new UpdateTableRequest(
        Collections.emptyList(), Collections.singletonList(new MetadataUpdate.AddSchema(schema)));
  }
}
