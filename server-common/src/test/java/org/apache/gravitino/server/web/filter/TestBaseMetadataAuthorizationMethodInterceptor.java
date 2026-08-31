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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests for {@link BaseMetadataAuthorizationMethodInterceptor}. */
public class TestBaseMetadataAuthorizationMethodInterceptor {

  private UserPrincipal principal;
  private GravitinoAuthorizer authorizer;
  private MockedStatic<PrincipalUtils> principalUtils;
  private MockedStatic<AuthorizationUtils> authorizationUtils;
  private MockedStatic<GravitinoAuthorizerProvider> authorizerProvider;

  @BeforeEach
  public void setUp() {
    principal = new UserPrincipal("tester");
    authorizer = mock(GravitinoAuthorizer.class);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);

    principalUtils = mockStatic(PrincipalUtils.class);
    principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
    principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn(principal.getName());

    authorizationUtils = mockStatic(AuthorizationUtils.class);
    authorizerProvider = mockStatic(GravitinoAuthorizerProvider.class);
    authorizerProvider.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    when(authorizer.deny(any(), any(), any(), any(), any())).thenReturn(false);
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(false);
    when(authorizer.findUnheldRoles(any(), any(), any(), any())).thenReturn(Set.of());
  }

  @AfterEach
  public void tearDown() {
    authorizerProvider.close();
    authorizationUtils.close();
    principalUtils.close();
  }

  @Test
  public void testDynamicTargetTypeDrivesCanAccessMetadata() throws Throwable {
    when(authorizer.authorize(any(), any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Privilege.Name privilege = invocation.getArgument(3);
              return privilege == Privilege.Name.USE_CATALOG
                  || privilege == Privilege.Name.USE_SCHEMA;
            });
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    assertEquals("authorized", interceptor.invoke(invocation));
    assertEquals("canAccessMetadata", interceptor.resolvedMethod.getName());
    verify(invocation).proceed();
    authorizationUtils.verify(
        () ->
            AuthorizationUtils.checkCurrentUser(
                eq("metalake"), eq("tester"), any(AuthorizationRequestContext.class)));
  }

  @Test
  public void testUserValidationFailureUsesProtocolMapper() throws Throwable {
    ForbiddenException failure = new ForbiddenException("not a metalake user");
    authorizationUtils
        .when(
            () ->
                AuthorizationUtils.checkCurrentUser(
                    eq("metalake"), eq("tester"), any(AuthorizationRequestContext.class)))
        .thenThrow(failure);
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    assertSame(failure, interceptor.invoke(invocation));
    verify(invocation, never()).proceed();
  }

  @Test
  public void testUserValidationSystemFailureRemainsInternalError() throws Throwable {
    IllegalStateException failure = new IllegalStateException("user store unavailable");
    authorizationUtils
        .when(
            () ->
                AuthorizationUtils.checkCurrentUser(
                    eq("metalake"), eq("tester"), any(AuthorizationRequestContext.class)))
        .thenThrow(failure);
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    RuntimeException response =
        assertInstanceOf(RuntimeException.class, interceptor.invoke(invocation));
    assertEquals("Failed to validate user", response.getMessage());
    assertSame(failure, response.getCause());
    verify(invocation, never()).proceed();
  }

  @Test
  public void testExpressionDenialUsesResolvedTarget() throws Throwable {
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    Object response = interceptor.invoke(invocation);

    ForbiddenException failure = assertInstanceOf(ForbiddenException.class, response);
    assertTrue(failure.getMessage().contains("metalake.catalog.schema"));
    verify(invocation, never()).proceed();
  }

  @Test
  public void testUnheldActiveRoleUsesProtocolMapper() throws Throwable {
    principal = principal.withActiveRoles(ActiveRoles.of(List.of("admin")));
    principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
    when(authorizer.findUnheldRoles(any(), any(), any(), any())).thenReturn(Set.of("admin"));
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    Object response = interceptor.invoke(invocation);

    ForbiddenException failure = assertInstanceOf(ForbiddenException.class, response);
    assertTrue(failure.getMessage().contains("admin"));
    verify(invocation, never()).proceed();
  }

  @Test
  public void testHeldActiveRoleContinuesAuthorization() throws Throwable {
    principal = principal.withActiveRoles(ActiveRoles.of(List.of("analyst")));
    principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.METALAKE);
    interceptor.skipExpressionEvaluation = true;
    TestInvocation invocation = invocation("alwaysDenied", "authorized");

    assertEquals("authorized", interceptor.invoke(invocation));
    verify(authorizer)
        .findUnheldRoles(
            eq(principal),
            eq("metalake"),
            eq(Set.of("analyst")),
            any(AuthorizationRequestContext.class));
  }

  @Test
  public void testCompletedHandlerSkipsExpressionEvaluation() throws Throwable {
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    interceptor.handler =
        Optional.of(
            new BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler() {
              @Override
              public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {}

              @Override
              public boolean authorizationCompleted() {
                return true;
              }
            });
    TestInvocation invocation = invocation("alwaysDenied", "authorized by handler");

    assertEquals("authorized by handler", interceptor.invoke(invocation));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  public void testExpressionOnlySkipStillValidatesUser() throws Throwable {
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.METALAKE);
    interceptor.skipExpressionEvaluation = true;
    TestInvocation invocation = invocation("alwaysDenied", "root list");

    assertEquals("root list", interceptor.invoke(invocation));
    authorizationUtils.verify(
        () ->
            AuthorizationUtils.checkCurrentUser(
                eq("metalake"), eq("tester"), any(AuthorizationRequestContext.class)));
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  public void testFullSkipBypassesLocalAuthorization() throws Throwable {
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    interceptor.skipAuthorization = true;
    TestInvocation invocation = invocation("alwaysDenied", "proxied");

    assertEquals("proxied", interceptor.invoke(invocation));
    authorizationUtils.verifyNoInteractions();
    verify(authorizer, never()).authorize(any(), any(), any(), any(), any());
  }

  @Test
  public void testMissingMetalakeIdentifierSkipsUserValidation() throws Throwable {
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.METALAKE);
    interceptor.includeMetalake = false;
    interceptor.skipExpressionEvaluation = true;
    TestInvocation invocation = invocation("alwaysDenied", "authorized");

    assertEquals("authorized", interceptor.invoke(invocation));
    authorizationUtils.verifyNoInteractions();
  }

  @Test
  public void testProtocolExceptionFromHandlerIsPreserved() throws Throwable {
    TestProtocolException failure = new TestProtocolException("bad protocol input");
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    interceptor.handler =
        Optional.of(
            new BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler() {
              @Override
              public void process(Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
                throw failure;
              }

              @Override
              public boolean authorizationCompleted() {
                return false;
              }
            });
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    assertSame(failure, interceptor.invoke(invocation));
    verify(invocation, never()).proceed();
  }

  @Test
  public void testInternalAuthorizationFailureIsWrapped() throws Throwable {
    IllegalStateException failure = new IllegalStateException("resolver failed");
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    interceptor.resolutionFailure = failure;
    TestInvocation invocation = invocation("canAccessMetadata", "authorized");

    RuntimeException response =
        assertInstanceOf(RuntimeException.class, interceptor.invoke(invocation));
    assertSame(failure, response.getCause());
    assertTrue(response.getMessage().contains("system internal error"));
    verify(invocation, never()).proceed();
  }

  @Test
  public void testOperationFailureUsesProtocolMapper() throws Throwable {
    IllegalStateException failure = new IllegalStateException("operation failed");
    TestInterceptor interceptor = new TestInterceptor(Entity.EntityType.SCHEMA);
    TestInvocation invocation = invocation("unannotated", null);
    when(invocation.proceed()).thenThrow(failure);

    assertSame(failure, interceptor.invoke(invocation));
  }

  private static TestInvocation invocation(String methodName, Object result) throws Throwable {
    Method method = TestOperations.class.getDeclaredMethod(methodName);
    TestInvocation invocation = mock(TestInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[0]);
    when(invocation.proceed()).thenReturn(result);
    return invocation;
  }

  private static class TestInterceptor extends BaseMetadataAuthorizationMethodInterceptor {
    private final Entity.EntityType targetType;
    private Optional<AuthorizationHandler> handler = Optional.empty();
    private Method resolvedMethod;
    private RuntimeException resolutionFailure;
    private boolean includeMetalake = true;
    private boolean skipAuthorization;
    private boolean skipExpressionEvaluation;

    private TestInterceptor(Entity.EntityType targetType) {
      this.targetType = targetType;
    }

    private Object invoke(TestInvocation invocation) throws Throwable {
      return authorizeMethod(
          invocation.getMethod(), invocation.getArguments(), invocation::proceed);
    }

    @Override
    protected AuthorizationTarget resolveAuthorizationTarget(
        Method method, AuthorizationExpression annotation, Parameter[] parameters, Object[] args) {
      resolvedMethod = method;
      if (resolutionFailure != null) {
        throw resolutionFailure;
      }
      Map<Entity.EntityType, NameIdentifier> identifiers = new EnumMap<>(Entity.EntityType.class);
      if (includeMetalake) {
        identifiers.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake("metalake"));
      }
      identifiers.put(
          Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog("metalake", "catalog"));
      identifiers.put(
          Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema("metalake", "catalog", "schema"));
      return new AuthorizationTarget(identifiers, targetType);
    }

    @Override
    protected Object toErrorResponse(Method method, Object[] args, Throwable throwable) {
      return throwable;
    }

    @Override
    protected Optional<AuthorizationHandler> createAuthorizationHandler(
        Method method, Parameter[] parameters, Object[] args) {
      return handler.isPresent()
          ? handler
          : super.createAuthorizationHandler(method, parameters, args);
    }

    @Override
    protected boolean isExceptionPropagate(Exception exception) {
      return exception instanceof TestProtocolException || super.isExceptionPropagate(exception);
    }

    @Override
    protected boolean shouldSkipAuthorization(
        Map<Entity.EntityType, NameIdentifier> nameIdentifierMap) {
      return skipAuthorization || super.shouldSkipAuthorization(nameIdentifierMap);
    }

    @Override
    protected boolean shouldSkipExpressionEvaluation(AuthorizationTarget target) {
      return skipExpressionEvaluation || super.shouldSkipExpressionEvaluation(target);
    }
  }

  private static class TestProtocolException extends RuntimeException {
    private TestProtocolException(String message) {
      super(message);
    }
  }

  private interface TestInvocation {
    Method getMethod();

    Object[] getArguments();

    Object proceed() throws Throwable;
  }

  private static class TestOperations {
    @AuthorizationExpression(
        expression = CAN_ACCESS_METADATA,
        accessMetadataType = MetadataObject.Type.METALAKE)
    private String canAccessMetadata() {
      return "unused";
    }

    @AuthorizationExpression(expression = "SCHEMA::CREATE_SCHEMA")
    private String alwaysDenied() {
      return "unused";
    }

    private String unannotated() {
      return "unused";
    }
  }
}
