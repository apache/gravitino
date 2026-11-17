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

package org.apache.gravitino.server.web.filter.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.GravitinoInterceptionService;
import org.apache.gravitino.server.web.rest.JobOperations;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class TestRunJobAuthorizationExecutor {

  @Test
  public void testNullRequestBodyThrowsBadRequest() throws Exception {
    RunJobAuthorizationExecutor executor = createExecutor(null);
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    FieldUtils.writeField(executor, "authorizationExpressionEvaluator", evaluator, true);

    BadRequestException thrown =
        assertThrows(
            BadRequestException.class, () -> executor.execute(new AuthorizationRequestContext()));
    assertEquals("Request body cannot be null", thrown.getMessage());
    verifyNoInteractions(evaluator);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  "})
  public void testMissingOrBlankJobTemplateNameThrowsBadRequest(String jobTemplateName)
      throws Exception {
    RunJobAuthorizationExecutor executor =
        createExecutor(new JobRunRequest(jobTemplateName, Collections.emptyMap()));
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    FieldUtils.writeField(executor, "authorizationExpressionEvaluator", evaluator, true);

    BadRequestException thrown =
        assertThrows(
            BadRequestException.class, () -> executor.execute(new AuthorizationRequestContext()));
    assertTrue(thrown.getMessage().contains("jobTemplateName"));
    verifyNoInteractions(evaluator);
  }

  @Test
  public void testValidRequestEvaluatesAuthorization() throws Exception {
    JobRunRequest request = new JobRunRequest("tpl", Collections.emptyMap());
    Map<Entity.EntityType, NameIdentifier> metadataContext = new HashMap<>();
    metadataContext.put(Entity.EntityType.METALAKE, NameIdentifier.of("ml"));
    RunJobAuthorizationExecutor executor = createExecutor(request, metadataContext);
    AuthorizationExpressionEvaluator evaluator = mock(AuthorizationExpressionEvaluator.class);
    when(evaluator.evaluate(anyMap(), anyMap(), any(AuthorizationRequestContext.class), any()))
        .thenReturn(true);
    FieldUtils.writeField(executor, "authorizationExpressionEvaluator", evaluator, true);

    AuthorizationRequestContext context = new AuthorizationRequestContext();
    assertTrue(executor.execute(context));
    assertEquals(
        NameIdentifierUtil.ofJobTemplate("ml", "tpl"),
        metadataContext.get(Entity.EntityType.JOB_TEMPLATE));
    verify(evaluator).evaluate(anyMap(), anyMap(), any(AuthorizationRequestContext.class), any());
  }

  @Test
  public void testInterceptorReturns400ForNullBody() throws Throwable {
    Response response = invokeRunJobThroughInterceptor(null);
    assertBadRequest(response, "Request body cannot be null");
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"  "})
  public void testInterceptorReturns400ForMissingOrBlankJobTemplateName(String jobTemplateName)
      throws Throwable {
    Response response =
        invokeRunJobThroughInterceptor(new JobRunRequest(jobTemplateName, Collections.emptyMap()));
    assertBadRequest(response, "jobTemplateName");
  }

  private static void assertBadRequest(Response response, String messageFragment) {
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertTrue(errorResponse.getMessage().contains(messageFragment));
    assertFalse(
        errorResponse.getMessage().contains("Authorization failed due to system internal error"));
  }

  private static Response invokeRunJobThroughInterceptor(JobRunRequest request) throws Throwable {
    Method method = JobOperations.class.getMethod("runJob", String.class, JobRunRequest.class);
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[] {"ml", request});

    try (MockedStatic<PrincipalUtils> principalUtils = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> authorizerProvider =
            mockStatic(GravitinoAuthorizerProvider.class);
        MockedStatic<AuthorizationUtils> authorizationUtils =
            mockStatic(AuthorizationUtils.class)) {
      principalUtils
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("admin"));
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn("admin");
      authorizationUtils
          .when(() -> AuthorizationUtils.checkCurrentUser(any(), any(), any()))
          .thenAnswer(ignored -> null);

      GravitinoAuthorizerProvider provider = mock(GravitinoAuthorizerProvider.class);
      authorizerProvider.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      when(provider.getGravitinoAuthorizer()).thenReturn(mock(GravitinoAuthorizer.class));

      MethodInterceptor interceptor =
          new GravitinoInterceptionService().getMethodInterceptors(method).get(0);
      Response response = (Response) interceptor.invoke(invocation);
      verify(invocation, never()).proceed();
      return response;
    }
  }

  private static RunJobAuthorizationExecutor createExecutor(JobRunRequest request)
      throws Exception {
    Map<Entity.EntityType, NameIdentifier> metadataContext = new HashMap<>();
    metadataContext.put(Entity.EntityType.METALAKE, NameIdentifier.of("ml"));
    return createExecutor(request, metadataContext);
  }

  private static RunJobAuthorizationExecutor createExecutor(
      JobRunRequest request, Map<Entity.EntityType, NameIdentifier> metadataContext)
      throws Exception {
    Method method = TestOperations.class.getMethod("runJob", JobRunRequest.class);
    return new RunJobAuthorizationExecutor(
        method.getParameters(),
        new Object[] {request},
        "METALAKE::OWNER",
        metadataContext,
        Collections.emptyMap(),
        Optional.empty());
  }

  @SuppressWarnings("unused")
  public static class TestOperations {
    public void runJob(
        @AuthorizationRequest(type = AuthorizationRequest.RequestType.RUN_JOB)
            JobRunRequest request) {}
  }
}
