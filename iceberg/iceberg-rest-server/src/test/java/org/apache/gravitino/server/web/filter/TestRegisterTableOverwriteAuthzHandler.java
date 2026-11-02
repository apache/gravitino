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
import java.util.Set;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergNamespaceOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests that Iceberg REST registerTable with {@code overwrite=true} requires table modify / owner,
 * not only schema {@code CREATE_TABLE}.
 */
class TestRegisterTableOverwriteAuthzHandler {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String TABLE = "existing_table";
  private static final String PROCEEDED = "PROCEEDED";

  private GravitinoAuthorizer authorizer;
  private IcebergMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn(METALAKE);
    when(configProvider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(configProvider, false, false, false, null);

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
  void testCreateTableAloneCannotOverwrite() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    MethodInvocation overwrite = registerInvocation(true);
    Object result = interceptor.invoke(overwrite);
    assertForbidden(result);
    verify(overwrite, never()).proceed();
  }

  @Test
  void testModifyTableAllowsOverwrite() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.MODIFY_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(true)));
  }

  @Test
  void testTableOwnerAllowsOverwriteWithoutModifyPrivilege() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA);
    ownerOf(MetadataObject.Type.TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(true)));
  }

  @Test
  void testCreateTableStillAllowsPlainRegister() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.USE_SCHEMA, Privilege.Name.CREATE_TABLE);

    assertEquals(PROCEEDED, interceptor.invoke(registerInvocation(false)));
  }

  private MethodInvocation registerInvocation(boolean overwrite) throws Throwable {
    Method method =
        IcebergNamespaceOperations.class.getMethod(
            "registerTable", String.class, String.class, RegisterTableRequest.class, String.class);
    RegisterTableRequest request =
        ImmutableRegisterTableRequest.builder()
            .name(TABLE)
            .metadataLocation("file:///tmp/metadata.json")
            .overwrite(overwrite)
            .build();
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[] {CATALOG + "/", SCHEMA, request, null});
    when(invocation.proceed()).thenReturn(PROCEEDED);
    return invocation;
  }

  private void ownerOf(MetadataObject.Type... types) {
    Set<MetadataObject.Type> owned = Set.of(types);
    doAnswer(
            invocation -> {
              MetadataObject metadataObject = invocation.getArgument(2);
              return metadataObject != null && owned.contains(metadataObject.type());
            })
        .when(authorizer)
        .isOwner(any(), any(), any(), any());
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
