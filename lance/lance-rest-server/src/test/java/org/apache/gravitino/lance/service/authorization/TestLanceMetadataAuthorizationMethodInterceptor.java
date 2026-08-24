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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.lance.service.authorization.annotations.LanceAuthorizationExpression;
import org.apache.gravitino.lance.service.authorization.annotations.LanceNamespaceDelimiter;
import org.apache.gravitino.lance.service.authorization.annotations.LanceNamespaceId;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Test for {@link LanceMetadataAuthorizationMethodInterceptor}. */
class TestLanceMetadataAuthorizationMethodInterceptor {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String PROCEEDED = "PROCEEDED";

  @BeforeEach
  void setUp() {
    LanceRESTServerContext.create(true, true, METALAKE);
  }

  @Test
  void testProceedWhenAuthorizationDisabled() throws Throwable {
    LanceRESTServerContext.create(false, true, METALAKE);
    RecordingInterceptor interceptor = new RecordingInterceptor(false);

    Object result = interceptor.invoke(describeInvocation(CATALOG, "$"));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertNull(interceptor.evaluatedExpression);
  }

  @Test
  void testProceedWhenNotRunningInAuxiliaryMode() throws Throwable {
    LanceRESTServerContext.create(true, false, METALAKE);
    RecordingInterceptor interceptor = new RecordingInterceptor(false);

    Object result = interceptor.invoke(describeInvocation(CATALOG, "$"));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertNull(interceptor.evaluatedExpression);
  }

  @Test
  void testCatalogLevelIdUsesCatalogExpression() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(true);

    Object result = interceptor.invoke(describeInvocation(CATALOG, "$"));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertEquals("catalog-expression", interceptor.evaluatedExpression);
    Assertions.assertEquals(
        NameIdentifierUtil.ofMetalake(METALAKE),
        interceptor.evaluatedIdentifiers.get(Entity.EntityType.METALAKE));
    Assertions.assertEquals(
        NameIdentifierUtil.ofCatalog(METALAKE, CATALOG),
        interceptor.evaluatedIdentifiers.get(Entity.EntityType.CATALOG));
    Assertions.assertFalse(
        interceptor.evaluatedIdentifiers.containsKey(Entity.EntityType.SCHEMA),
        "A catalog-level identifier must not address a schema");
  }

  @Test
  void testSchemaLevelIdUsesSchemaExpression() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(true);

    Object result = interceptor.invoke(describeInvocation(CATALOG + "$" + SCHEMA, "$"));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertEquals("schema-expression", interceptor.evaluatedExpression);
    Assertions.assertEquals(
        NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA),
        interceptor.evaluatedIdentifiers.get(Entity.EntityType.SCHEMA));
  }

  @Test
  void testCustomDelimiterIsHonored() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(true);

    Object result = interceptor.invoke(describeInvocation(CATALOG + "." + SCHEMA, "."));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertEquals(
        NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA),
        interceptor.evaluatedIdentifiers.get(Entity.EntityType.SCHEMA));
  }

  @Test
  void testDeniedRequestReturnsForbiddenAndDoesNotProceed() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(false);
    MethodInvocation invocation = describeInvocation(CATALOG, "$");

    Object result = interceptor.invoke(invocation);

    assertErrorResponse(result, Response.Status.FORBIDDEN.getStatusCode());
    Mockito.verify(invocation, Mockito.never()).proceed();
  }

  @Test
  void testRootNamespaceIsRejectedWhenNotAllowed() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(true);
    MethodInvocation invocation = describeInvocation("", "$");

    Object result = interceptor.invoke(invocation);

    assertErrorResponse(result, Response.Status.BAD_REQUEST.getStatusCode());
    Mockito.verify(invocation, Mockito.never()).proceed();
    Assertions.assertNull(interceptor.evaluatedExpression);
  }

  @Test
  void testTooManyLevelsAreRejected() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(true);
    MethodInvocation invocation = describeInvocation(CATALOG + "$" + SCHEMA + "$extra", "$");

    Object result = interceptor.invoke(invocation);

    assertErrorResponse(result, Response.Status.BAD_REQUEST.getStatusCode());
    Mockito.verify(invocation, Mockito.never()).proceed();
  }

  @Test
  void testRootNamespaceListingSkipsExpressionEvaluation() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(false);

    Object result = interceptor.invoke(listRootInvocation("$"));

    Assertions.assertEquals(PROCEEDED, result);
    Assertions.assertNull(
        interceptor.evaluatedExpression, "The root namespace holds no privileges of its own");
  }

  @Test
  void testUnheldActiveRolesAreRejected() throws Throwable {
    UserPrincipal principal =
        new UserPrincipal("tester")
            .withActiveRoles(ActiveRoles.of(Collections.singletonList("ghost_role")));
    GravitinoAuthorizer authorizer = Mockito.mock(GravitinoAuthorizer.class);
    Mockito.when(
            authorizer.findUnheldRoles(
                Mockito.any(), Mockito.eq(METALAKE), Mockito.any(), Mockito.any()))
        .thenReturn(Collections.singleton("ghost_role"));

    try (MockedStatic<PrincipalUtils> principalUtils = Mockito.mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            Mockito.mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      GravitinoAuthorizerProvider provider = Mockito.mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      Mockito.when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);

      RecordingInterceptor interceptor = new RecordingInterceptor(true);
      MethodInvocation invocation = describeInvocation(CATALOG, "$");

      Object result = interceptor.invoke(invocation);

      assertErrorResponse(result, Response.Status.FORBIDDEN.getStatusCode());
      Mockito.verify(invocation, Mockito.never()).proceed();
      Assertions.assertNull(interceptor.evaluatedExpression);
    }
  }

  @Test
  void testUnexpectedAuthorizationErrorIsMappedToInternalError() throws Throwable {
    LanceMetadataAuthorizationMethodInterceptor interceptor =
        new LanceMetadataAuthorizationMethodInterceptor() {
          @Override
          boolean evaluateExpression(
              String expression,
              Map<Entity.EntityType, NameIdentifier> nameIdentifiers,
              AuthorizationRequestContext requestContext) {
            throw new IllegalStateException("authorizer is broken");
          }
        };
    MethodInvocation invocation = describeInvocation(CATALOG, "$");

    Object result = interceptor.invoke(invocation);

    assertErrorResponse(result, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    Mockito.verify(invocation, Mockito.never()).proceed();
  }

  @Test
  void testMethodWithoutAnnotationIsNotAuthorized() throws Throwable {
    RecordingInterceptor interceptor = new RecordingInterceptor(false);
    MethodInvocation invocation =
        invocation(TestOperations.class.getMethod("unannotated", String.class), CATALOG);

    Assertions.assertEquals(PROCEEDED, interceptor.invoke(invocation));
    Assertions.assertNull(interceptor.evaluatedExpression);
  }

  private void assertErrorResponse(Object result, int expectedStatus) {
    Assertions.assertInstanceOf(Response.class, result);
    Response response = (Response) result;
    Assertions.assertEquals(expectedStatus, response.getStatus());
    Assertions.assertInstanceOf(ErrorResponse.class, response.getEntity());
  }

  private MethodInvocation describeInvocation(String namespaceId, String delimiter)
      throws NoSuchMethodException {
    return invocation(
        TestOperations.class.getMethod("describeNamespace", String.class, String.class),
        namespaceId,
        delimiter);
  }

  private MethodInvocation listRootInvocation(String delimiter) throws NoSuchMethodException {
    return invocation(
        TestOperations.class.getMethod("listNamespacesOnRoot", String.class), delimiter);
  }

  private MethodInvocation invocation(Method method, Object... args) {
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments()).thenReturn(args);
    try {
      Mockito.when(invocation.proceed()).thenReturn(PROCEEDED);
    } catch (Throwable e) {
      throw new IllegalStateException(e);
    }
    return invocation;
  }

  /** An interceptor that records the evaluated expression instead of calling the authorizer. */
  private static class RecordingInterceptor extends LanceMetadataAuthorizationMethodInterceptor {

    private final boolean authorized;
    private String evaluatedExpression;
    private Map<Entity.EntityType, NameIdentifier> evaluatedIdentifiers = new HashMap<>();

    private RecordingInterceptor(boolean authorized) {
      this.authorized = authorized;
    }

    @Override
    boolean evaluateExpression(
        String expression,
        Map<Entity.EntityType, NameIdentifier> nameIdentifiers,
        AuthorizationRequestContext requestContext) {
      this.evaluatedExpression = expression;
      this.evaluatedIdentifiers = nameIdentifiers;
      return authorized;
    }
  }

  /** Stand-in for the Lance REST resource, carrying the authorization annotations under test. */
  public static class TestOperations {

    @LanceAuthorizationExpression(
        catalogExpression = "catalog-expression",
        schemaExpression = "schema-expression")
    public Response describeNamespace(
        @LanceNamespaceId String namespaceId, @LanceNamespaceDelimiter String delimiter) {
      return Response.ok().build();
    }

    @LanceAuthorizationExpression(
        catalogExpression = "catalog-expression",
        schemaExpression = "schema-expression",
        allowRootNamespace = true)
    public Response listNamespacesOnRoot(@LanceNamespaceDelimiter String delimiter) {
      return Response.ok().build();
    }

    public Response unannotated(String namespaceId) {
      return Response.ok().build();
    }
  }
}
