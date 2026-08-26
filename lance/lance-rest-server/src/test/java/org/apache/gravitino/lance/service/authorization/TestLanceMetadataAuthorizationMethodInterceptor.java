/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Set;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.lance.service.authorization.annotations.LanceRootNamespace;
import org.apache.gravitino.lance.service.rest.LanceNamespaceOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;
import org.mockito.MockedStatic;

/** Tests the Lance-specific target resolution and response mapping around the shared pipeline. */
class TestLanceMetadataAuthorizationMethodInterceptor {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String PROCEEDED = "PROCEEDED";

  private GravitinoAuthorizer authorizer;
  private LanceMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  void setUp() {
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
    interceptor = new LanceMetadataAuthorizationMethodInterceptor(METALAKE);
  }

  @AfterEach
  void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  void testCreateSchemaPrivilegeAllowsProbeButNotDescribe() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);

    assertEquals(
        PROCEEDED,
        interceptor.invoke(invocation(namespaceMethod("namespaceExists"), CATALOG, "$")));
    assertEquals(
        PROCEEDED,
        interceptor.invoke(
            invocation(namespaceMethod("namespaceExists"), CATALOG + "." + SCHEMA, ".")));

    Object describe =
        interceptor.invoke(
            invocation(namespaceMethod("describeNamespace"), CATALOG + "." + SCHEMA, "."));
    assertErrorResponse(describe, Response.Status.FORBIDDEN);
  }

  @Test
  void testDenyOverridesSchemaProbePrivileges() throws Throwable {
    allow(Privilege.Name.USE_CATALOG, Privilege.Name.CREATE_SCHEMA);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(true);

    Object result =
        interceptor.invoke(
            invocation(namespaceMethod("namespaceExists"), CATALOG + "$" + SCHEMA, "$"));
    assertErrorResponse(result, Response.Status.FORBIDDEN);
  }

  @Test
  void testRootValidatesUserButSkipsPrivilegeExpression() throws Throwable {
    Method rootMethod =
        LanceNamespaceOperations.class.getMethod(
            "listNamespacesOnRoot", String.class, String.class, Integer.class);
    MethodInvocation invocation = invocation(rootMethod, "$", null, null);
    assertEquals(PROCEEDED, interceptor.invoke(invocation));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  void testProtocolAndOperationFailuresUseLanceResponses() throws Throwable {
    MethodInvocation invalid =
        invocation(namespaceMethod("describeNamespace"), CATALOG + "$" + SCHEMA + "$extra", "$");
    assertErrorResponse(interceptor.invoke(invalid), Response.Status.BAD_REQUEST);
    verify(invalid, never()).proceed();

    MethodInvocation empty = invocation(namespaceMethod("describeNamespace"), "", "$");
    assertErrorResponse(interceptor.invoke(empty), Response.Status.BAD_REQUEST);

    MethodInvocation missing =
        invocation(TestOperations.class.getMethod("missingTarget", String.class), "$");
    assertErrorResponse(interceptor.invoke(missing), Response.Status.INTERNAL_SERVER_ERROR);

    MethodInvocation conflicting =
        invocation(TestOperations.class.getMethod("conflictingRoot", String.class), CATALOG);
    assertErrorResponse(interceptor.invoke(conflicting), Response.Status.INTERNAL_SERVER_ERROR);

    MethodInvocation operation =
        invocation(TestOperations.class.getMethod("unannotated"), new Object[0]);
    when(operation.proceed()).thenThrow(new IllegalArgumentException("bad request"));
    assertErrorResponse(interceptor.invoke(operation), Response.Status.BAD_REQUEST);
  }

  private void allow(Privilege.Name... privileges) {
    Set<Privilege.Name> allowed = Set.of(privileges);
    when(authorizer.authorize(any(), any(), any(), any(), any()))
        .thenAnswer(invocation -> allowed.contains(invocation.getArgument(3)));
  }

  private Method namespaceMethod(String name) throws NoSuchMethodException {
    return LanceNamespaceOperations.class.getMethod(name, String.class, String.class);
  }

  private MethodInvocation invocation(Method method, Object... args) throws Throwable {
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(args);
    when(invocation.proceed()).thenReturn(PROCEEDED);
    return invocation;
  }

  private void assertErrorResponse(Object result, Response.Status expectedStatus) {
    Response response = assertInstanceOf(Response.class, result);
    assertEquals(expectedStatus.getStatusCode(), response.getStatus());
    assertInstanceOf(ErrorResponse.class, response.getEntity());
  }

  public static class TestOperations {
    @AuthorizationExpression(expression = "CAN_ACCESS_METADATA")
    public void missingTarget(@QueryParam("delimiter") String delimiter) {}

    @AuthorizationExpression(expression = "CAN_ACCESS_METADATA")
    @LanceRootNamespace
    public void conflictingRoot(@PathParam("id") String namespaceId) {}

    public void unannotated() {}
  }
}
