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
package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergTableOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests staged-create authorization separately from ordinary table commits. */
class TestUpdateTableAuthzHandler {
  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String TABLE = "existing_table";
  private static final String PROCEEDED = "PROCEEDED";

  private CatalogWrapperForREST catalogWrapper;
  private GravitinoAuthorizer authorizer;
  private IcebergMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
    catalogWrapper = mock(CatalogWrapperForREST.class);
    when(catalogWrapper.isRESTCatalog()).thenReturn(false);

    IcebergCatalogWrapperManager wrapperManager = mock(IcebergCatalogWrapperManager.class);
    when(wrapperManager.getCatalogWrapper(CATALOG)).thenReturn(catalogWrapper);

    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn(METALAKE);
    when(configProvider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(configProvider, true, false, false, wrapperManager);

    authorizer = mock(GravitinoAuthorizer.class);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    principalUtils = mockStatic(PrincipalUtils.class);
    authorizationUtils = mockStatic(AuthorizationUtils.class);
    authorizerProvider = mockStatic(GravitinoAuthorizerProvider.class);

    principalUtils
        .when(PrincipalUtils::getCurrentPrincipal)
        .thenReturn(new UserPrincipal("tester"));
    principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
    authorizerProvider.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(false);
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(false);
    when(authorizer.findUnheldRoles(any(), any(), any(), any())).thenReturn(Set.of());
    interceptor = new IcebergMetadataAuthorizationMethodInterceptor();
  }

  @AfterEach
  void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  void testDataUpdatesRequireWriteTable() throws Throwable {
    for (Class<? extends MetadataUpdate> type :
        List.of(
            MetadataUpdate.AddSnapshot.class, MetadataUpdate.RemoveSnapshots.class,
            MetadataUpdate.SetSnapshotRef.class, MetadataUpdate.RemoveSnapshotRef.class,
            MetadataUpdate.SetStatistics.class, MetadataUpdate.RemoveStatistics.class,
            MetadataUpdate.SetPartitionStatistics.class,
                MetadataUpdate.RemovePartitionStatistics.class)) {
      UpdateTableRequest request = updateRequest(mock(type));
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.WRITE_TABLE);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)), type.getSimpleName());
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.ALTER);
      MethodInvocation denied = updateInvocation(request);
      assertForbidden(interceptor.invoke(denied));
      verify(denied, never()).proceed();
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)));
    }
  }

  @Test
  void testMetadataUpdatesRequireAlter() throws Throwable {
    for (Class<? extends MetadataUpdate> type :
        List.of(
            MetadataUpdate.AssignUUID.class,
            MetadataUpdate.UpgradeFormatVersion.class,
            MetadataUpdate.AddSchema.class,
            MetadataUpdate.SetCurrentSchema.class,
            MetadataUpdate.RemoveSchemas.class,
            MetadataUpdate.AddPartitionSpec.class,
            MetadataUpdate.SetDefaultPartitionSpec.class,
            MetadataUpdate.RemovePartitionSpecs.class,
            MetadataUpdate.AddSortOrder.class,
            MetadataUpdate.SetDefaultSortOrder.class,
            MetadataUpdate.SetProperties.class,
            MetadataUpdate.RemoveProperties.class,
            MetadataUpdate.SetLocation.class,
            MetadataUpdate.AddEncryptionKey.class,
            MetadataUpdate.RemoveEncryptionKey.class)) {
      UpdateTableRequest request = updateRequest(mock(type));
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.ALTER);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)), type.getSimpleName());
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.WRITE_TABLE);
      assertForbidden(interceptor.invoke(updateInvocation(request)));
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)));
    }
  }

  @Test
  void testMixedAndUnknownUpdatesRequireBothPrivileges() throws Throwable {
    for (UpdateTableRequest request :
        List.of(
            updateRequest(
                mock(MetadataUpdate.AddSchema.class), mock(MetadataUpdate.AddSnapshot.class)),
            updateRequest(mock(MetadataUpdate.class)))) {
      for (Privilege.Name privilege : List.of(Privilege.Name.ALTER, Privilege.Name.WRITE_TABLE)) {
        allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, privilege);
        assertForbidden(interceptor.invoke(updateInvocation(request)));
      }
      allow(
          Privilege.Name.USE_CATALOG,
          Privilege.Name.USE_SCHEMA,
          Privilege.Name.ALTER,
          Privilege.Name.WRITE_TABLE);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)));
      allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
      assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)));
    }
  }

  @Test
  void testGranularDenialsBlockCommits() throws Throwable {
    allow(
        Privilege.Name.USE_CATALOG,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.ALTER,
        Privilege.Name.WRITE_TABLE);
    for (Privilege.Name denied : List.of(Privilege.Name.ALTER, Privilege.Name.WRITE_TABLE)) {
      doAnswer(invocation -> invocation.getArgument(3) == denied)
          .when(authorizer)
          .deny(any(), any(), any(), any(), any());
      assertForbidden(
          interceptor.invoke(
              updateInvocation(
                  updateRequest(
                      mock(MetadataUpdate.AddSchema.class),
                      mock(MetadataUpdate.AddSnapshot.class)))));
    }
  }

  private static UpdateTableRequest updateRequest(MetadataUpdate... updates) {
    return new UpdateTableRequest(Collections.emptyList(), List.of(updates));
  }

  @Test
  void testCreateOnlyUserCanCommitStagedCreate() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(true)));
  }

  @Test
  void testModifyOnlyUserCannotCreateTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    MethodInvocation invocation = updateInvocation(true);
    assertForbidden(interceptor.invoke(invocation));
    verify(invocation, never()).proceed();
  }

  @Test
  void testOrdinaryCommitStillRequiresModifyTable() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    MethodInvocation invocation = updateInvocation(false);
    assertForbidden(interceptor.invoke(invocation));
    verify(invocation, never()).proceed();
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);
    assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(false)));
  }

  @Test
  void testCreateCommitNeedsUseSchemaAndCatalog() throws Throwable {
    allow(Privilege.Name.CREATE_TABLE);
    assertForbidden(interceptor.invoke(updateInvocation(true)));
    allow(Privilege.Name.CREATE_TABLE, Privilege.Name.USE_CATALOG);
    assertForbidden(interceptor.invoke(updateInvocation(true)));
    allow(Privilege.Name.CREATE_TABLE, Privilege.Name.USE_SCHEMA);
    assertForbidden(interceptor.invoke(updateInvocation(true)));
  }

  @Test
  void testDeniedCreateCannotFallBackToModify() throws Throwable {
    allow(
        Privilege.Name.USE_CATALOG,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.MODIFY_TABLE);
    doAnswer(invocation -> invocation.getArgument(3) == Privilege.Name.CREATE_TABLE)
        .when(authorizer)
        .deny(any(), any(), any(), any(), any());
    assertForbidden(interceptor.invoke(updateInvocation(true)));
  }

  @Test
  void testCreateCommitChecksSchemaOwnership() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);
    when(authorizer.isOwner(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              return object != null && object.type() == MetadataObject.Type.TABLE;
            });
    assertForbidden(interceptor.invoke(updateInvocation(true)));
    when(authorizer.isOwner(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              return object != null && object.type() == MetadataObject.Type.SCHEMA;
            });
    assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(true)));
  }

  @Test
  void testStagedCreateRequirementCanAppearAfterOtherRequirements() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);
    UpdateTableRequest request =
        new UpdateTableRequest(
            List.of(
                new UpdateRequirement.AssertTableUUID("uuid"),
                new UpdateRequirement.AssertTableDoesNotExist()),
            Collections.emptyList());
    assertEquals(PROCEEDED, interceptor.invoke(updateInvocation(request)));
  }

  @Test
  void testHandlerAttributesCreateDenialToSchema() {
    assertEquals(EntityType.SCHEMA, new UpdateTableAuthzHandler(request(true)).entityType());
    assertEquals(EntityType.TABLE, new UpdateTableAuthzHandler(request(false)).entityType());
  }

  private MethodInvocation updateInvocation(boolean create) throws Throwable {
    return updateInvocation(request(create));
  }

  private MethodInvocation updateInvocation(UpdateTableRequest request) throws Throwable {
    Method method =
        IcebergTableOperations.class.getMethod(
            "updateTable", String.class, String.class, String.class, UpdateTableRequest.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[] {CATALOG + "/", "s1", TABLE, request});
    when(invocation.proceed()).thenReturn(PROCEEDED);
    return invocation;
  }

  private static UpdateTableRequest request(boolean create) {
    return new UpdateTableRequest(
        create ? List.of(new UpdateRequirement.AssertTableDoesNotExist()) : Collections.emptyList(),
        Collections.emptyList());
  }

  private void allow(Privilege.Name... privileges) {
    Set<Privilege.Name> allowed = Set.of(privileges);
    doAnswer(invocation -> allowed.contains(invocation.getArgument(3)))
        .when(authorizer)
        .authorize(any(), any(), any(), any(), any());
  }

  private static void assertForbidden(Object result) {
    Response response = assertInstanceOf(Response.class, result);
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertInstanceOf(ErrorResponse.class, response.getEntity());
  }
}
