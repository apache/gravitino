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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergTableOperations;
import org.apache.gravitino.iceberg.service.rest.IcebergViewOperations;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.server.AuthorizationDenialFailureEvent;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class TestIcebergLoadAuthorizationAudit {
  private GravitinoAuthorizer authorizer;
  private EventBus eventBus;
  private IcebergCatalogWrapperManager wrapperManager;
  private IcebergMetadataAuthorizationMethodInterceptor interceptor;
  private MockedStatic<PrincipalUtils> principals;
  private MockedStatic<AuthorizationUtils> authorization;
  private MockedStatic<GravitinoAuthorizerProvider> providers;
  private MockedStatic<GravitinoEnv> environments;

  @BeforeEach
  void setUp() {
    IcebergConfigProvider configProvider = mock(IcebergConfigProvider.class);
    when(configProvider.getMetalakeName()).thenReturn("ml");
    when(configProvider.getDefaultCatalogName()).thenReturn("cat");
    wrapperManager = mock(IcebergCatalogWrapperManager.class);
    when(wrapperManager.getCatalogWrapper("cat")).thenReturn(mock(CatalogWrapperForREST.class));
    IcebergRESTServerContext.create(configProvider, false, false, true, wrapperManager);

    authorizer = mock(GravitinoAuthorizer.class);
    GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
    when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);
    providers = mockStatic(GravitinoAuthorizerProvider.class);
    providers.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
    principals = mockStatic(PrincipalUtils.class);
    principals.when(PrincipalUtils::getCurrentPrincipal).thenReturn(new UserPrincipal("reader"));
    principals.when(PrincipalUtils::getCurrentUserName).thenReturn("reader");
    authorization = mockStatic(AuthorizationUtils.class);

    eventBus = mock(EventBus.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.eventBus()).thenReturn(eventBus);
    environments = mockStatic(GravitinoEnv.class);
    environments.when(GravitinoEnv::getInstance).thenReturn(env);
    interceptor = new IcebergMetadataAuthorizationMethodInterceptor();
  }

  @AfterEach
  void tearDown() {
    environments.close();
    authorization.close();
    principals.close();
    providers.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"loadTable", "loadView"})
  void testLoadDenialRecordsObjectAndExpression(String operation) throws Throwable {
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(403, response.getStatus());
      assertEquals("ForbiddenException", ((ErrorResponse) response.getEntity()).type());
    }
    ArgumentCaptor<BaseEvent> event = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(event.capture());
    AuthorizationDenialFailureEvent denial =
        assertInstanceOf(AuthorizationDenialFailureEvent.class, event.getValue());
    assertEquals("reader", denial.user());
    assertEquals(NameIdentifier.of("ml", "cat", "s", "object"), denial.identifier());
    assertEquals(operation, denial.methodName());
    assertEquals(EventSource.GRAVITINO_ICEBERG_REST_SERVER, denial.eventSource());
    assertEquals(
        invocation.getMethod().getAnnotation(AuthorizationExpression.class).expression(),
        denial.expression());
    verify(invocation, never()).proceed();
  }

  @ParameterizedTest
  @ValueSource(strings = {"loadTable", "loadView"})
  void testAllowedLoadDoesNotRecordDenial(String operation) throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    when(invocation.proceed()).thenReturn("loaded");
    assertEquals("loaded", interceptor.invoke(invocation));
    verify(eventBus, never()).dispatchEvent(any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"loadTable", "loadView"})
  void testBackendForbiddenDuringAuthorizationDoesNotRecordDenial(String operation)
      throws Throwable {
    when(wrapperManager.getCatalogWrapper("cat"))
        .thenThrow(new ForbiddenException("Backend credentials rejected"));
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(403, response.getStatus());
      assertEquals(
          "Backend credentials rejected", ((ErrorResponse) response.getEntity()).message());
    }
    verify(eventBus, never()).dispatchEvent(any());
    verify(invocation, never()).proceed();
  }

  @ParameterizedTest
  @ValueSource(strings = {"loadTable", "loadView"})
  void testBackendForbiddenAfterAuthorizationDoesNotRecordDenial(String operation)
      throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    when(invocation.proceed()).thenThrow(new ForbiddenException("Backend credentials rejected"));
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(403, response.getStatus());
      assertEquals(
          "Backend credentials rejected", ((ErrorResponse) response.getEntity()).message());
    }
    verify(eventBus, never()).dispatchEvent(any());
    verify(invocation).proceed();
  }

  @ParameterizedTest
  @CsvSource({
    "loadTable,false,403",
    "loadView,false,403",
    "loadTable,true,400",
    "loadView,true,400"
  })
  void testNonIcebergCatalogIsClientErrorOnlyForVisibleCatalog(
      String operation, boolean owner, int status) throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(owner);
    when(wrapperManager.getCatalogWrapper("cat"))
        .thenThrow(new IllegalArgumentException("Catalog cat is not an Iceberg catalog"));
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(status, response.getStatus());
    }
    verify(invocation, never()).proceed();
  }

  @ParameterizedTest
  @CsvSource({
    "loadTable,false,403",
    "loadView,false,403",
    "loadTable,true,404",
    "loadView,true,404"
  })
  void testMissingCatalogDoesNotBecomeInternalError(String operation, boolean owner, int status)
      throws Throwable {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(owner);
    when(wrapperManager.getCatalogWrapper("cat"))
        .thenThrow(new NoSuchCatalogException("Catalog cat does not exist"));
    MethodInvocation invocation = loadInvocation(operation, "cat/");
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(status, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      assertEquals(owner ? "NoSuchNamespaceException" : "ForbiddenException", error.type());
      assertEquals(
          owner
              ? "No Iceberg configuration for catalog 'cat'"
              : "Not authorized to access catalog 'cat'",
          error.message());
    }
    verify(invocation, never()).proceed();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCatalogLookupFailurePreservesCause(boolean missing) {
    when(authorizer.isOwner(any(), any(), any(), any())).thenReturn(true);
    RuntimeException cause =
        missing
            ? new NoSuchCatalogException("Catalog cat does not exist")
            : new IllegalArgumentException("Catalog cat is not an Iceberg catalog");
    when(wrapperManager.getCatalogWrapper("cat")).thenThrow(cause);
    Class<? extends RuntimeException> expectedType =
        missing ? NoSuchNamespaceException.class : BadRequestException.class;

    RuntimeException failure =
        assertThrows(
            expectedType,
            () ->
                IcebergLoadAuthzHandlerHelper.getCatalogWrapper(
                    "cat",
                    Map.of(
                        EntityType.METALAKE, NameIdentifier.of("ml"),
                        EntityType.CATALOG, NameIdentifier.of("ml", "cat"))));

    assertSame(cause, failure.getCause());
    assertEquals(
        missing ? "No Iceberg configuration for catalog 'cat'" : cause.getMessage(),
        failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"loadTable", "loadView"})
  void testMalformedCatalogPrefixIsBadRequest(String operation) throws Throwable {
    MethodInvocation invocation = loadInvocation(operation, "ml.cat/");
    try (Response response = (Response) interceptor.invoke(invocation)) {
      assertEquals(400, response.getStatus());
      assertEquals("BadRequestException", ((ErrorResponse) response.getEntity()).type());
    }
    verify(wrapperManager, never()).getCatalogWrapper(any());
    verify(eventBus, never()).dispatchEvent(any());
    verify(invocation, never()).proceed();
  }

  private MethodInvocation loadInvocation(String operation, String prefix) throws Exception {
    boolean table = operation.equals("loadTable");
    Method method =
        table
            ? IcebergTableOperations.class.getMethod(
                operation,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class)
            : IcebergViewOperations.class.getMethod(
                operation, String.class, String.class, String.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments())
        .thenReturn(
            table
                ? new Object[] {prefix, "s", "object", "all", null, null}
                : new Object[] {prefix, "s", "object"});
    return invocation;
  }
}
