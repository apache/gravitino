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

package org.apache.gravitino.server.authentication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRoles;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthenticationFilter {

  @Test
  public void testDoFilterNormal() throws ServletException, IOException {

    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testDoFilterSetsActiveRolesFromHeader() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(mockRequest.getHeader(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER))
        .thenReturn("analyst,reader");
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    // The active roles must be visible on the principal to the downstream chain (where
    // authorization runs).
    AtomicReference<Principal> seenDuringChain = new AtomicReference<>();
    FilterChain capturingChain =
        (req, resp) -> seenDuringChain.set(PrincipalUtils.getCurrentPrincipal());
    filter.doFilter(mockRequest, mockResponse, capturingChain);

    Assertions.assertInstanceOf(UserPrincipal.class, seenDuringChain.get());
    Assertions.assertEquals(
        ActiveRoles.of(Arrays.asList("analyst", "reader")),
        ((UserPrincipal) seenDuringChain.get()).getActiveRoles());
  }

  @Test
  public void testDoFilterDefaultsToAllWhenHeaderAbsent() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    AtomicReference<Principal> seenDuringChain = new AtomicReference<>();
    FilterChain capturingChain =
        (req, resp) -> seenDuringChain.set(PrincipalUtils.getCurrentPrincipal());
    filter.doFilter(mockRequest, mockResponse, capturingChain);

    // No header means today's behavior: every role the caller holds is active.
    Assertions.assertInstanceOf(UserPrincipal.class, seenDuringChain.get());
    Assertions.assertEquals(
        ActiveRoles.all(), ((UserPrincipal) seenDuringChain.get()).getActiveRoles());
  }

  @Test
  public void testDoFilterRejectsMalformedActiveRolesHeader() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(mockResponse.getWriter()).thenReturn(printWriter);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    // A reserved keyword combined with a role name is syntactically invalid.
    when(mockRequest.getHeader(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER))
        .thenReturn("ALL,analyst");
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    filter.doFilter(mockRequest, mockResponse, mockChain);

    verify(mockResponse).setStatus(HttpServletResponse.SC_BAD_REQUEST);
    verify(mockChain, never()).doFilter(any(), any());
  }

  @Test
  public void testDoFilterWithException() throws ServletException, IOException {
    Authenticator authenticator = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator), false);
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(mockResponse.getWriter()).thenReturn(printWriter);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(mockResponse).setContentType("application/json");
    verify(mockResponse).setCharacterEncoding("UTF-8");

    printWriter.flush();
    String json = stringWriter.toString();
    ObjectMapper mapper = ObjectMapperProvider.objectMapper(false);
    Assertions.assertFalse(mapper.readTree(json).has("stack"));
    ErrorResponse errorResponse = mapper.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(1011, errorResponse.getCode());
    Assertions.assertEquals("UnauthorizedException", errorResponse.getType());
    Assertions.assertEquals("UNAUTHORIZED", errorResponse.getMessage());
  }

  @Test
  public void testAuthErrorIncludesStackWhenEnabled() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(), true);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new UnauthorizedException("UNAUTHORIZED"));

    printWriter.flush();
    Assertions.assertTrue(
        ObjectMapperProvider.objectMapper(true).readTree(stringWriter.toString()).has("stack"));
  }

  @Test
  public void testMultiFilterNormal() throws ServletException, IOException {

    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any())).thenReturn(new UserPrincipal("user"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse, never()).sendError(anyInt(), anyString());
  }

  @Test
  public void testMultiFilterWithException() throws ServletException, IOException {

    Authenticator authenticator1 = mock(Authenticator.class);
    Authenticator authenticator2 = mock(Authenticator.class);
    AuthenticationFilter filter =
        new AuthenticationFilter(Lists.newArrayList(authenticator1, authenticator2));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(mockResponse.getWriter()).thenReturn(printWriter);
    when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    when(authenticator1.supportsToken(any())).thenReturn(false);
    when(authenticator1.isDataFromToken()).thenReturn(true);
    when(authenticator1.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    when(authenticator2.supportsToken(any())).thenReturn(true);
    when(authenticator2.isDataFromToken()).thenReturn(true);
    when(authenticator2.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));

    filter.doFilter(mockRequest, mockResponse, mockChain);
    verify(mockResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(mockResponse).setContentType("application/json");

    printWriter.flush();
    String json = stringWriter.toString();
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();
    ErrorResponse errorResponse = mapper.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(1011, errorResponse.getCode());
    Assertions.assertEquals("UnauthorizedException", errorResponse.getType());
    Assertions.assertEquals("UNAUTHORIZED", errorResponse.getMessage());
  }

  @Test
  public void testDoFilterBypassesAuthenticationForHealthEndpoints()
      throws ServletException, IOException {
    // /health, /health/live, /health/ready are root-level aliases; during a Jetty forward
    // getRequestURI() returns the original URI so the bypass must also match /health/* directly.
    String[] healthPaths = {
      "/health",
      "/health/live",
      "/health/ready",
      "/health.html",
      "/api/health",
      "/api/health/",
      "/api/health/live",
      "/api/health/ready"
    };
    for (String path : healthPaths) {
      Authenticator authenticator = mock(Authenticator.class);
      AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
      FilterChain mockChain = mock(FilterChain.class);
      HttpServletRequest mockRequest = mock(HttpServletRequest.class);
      HttpServletResponse mockResponse = mock(HttpServletResponse.class);
      when(mockRequest.getRequestURI()).thenReturn(path);

      filter.doFilter(mockRequest, mockResponse, mockChain);

      // Chain proceeds, no error response, and authenticator is never consulted.
      verify(mockChain).doFilter(mockRequest, mockResponse);
      verify(mockResponse, never()).sendError(anyInt(), anyString());
      verify(authenticator, never()).supportsToken(any());
    }
  }

  @Test
  public void testDoFilterDoesNotBypassAuthenticationForNonHealthPaths()
      throws ServletException, IOException {
    // Regression guard against an overly broad exemption. Paths that merely contain
    // "health" or share a prefix with "/api/health" must still be authenticated.
    String[] nonHealthPaths = {
      "/api/metalakes/health_metalake", "/api/healthcheck", "/api/version", "/api/metalakes"
    };
    for (String path : nonHealthPaths) {
      Authenticator authenticator = mock(Authenticator.class);
      AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
      FilterChain mockChain = mock(FilterChain.class);
      HttpServletRequest mockRequest = mock(HttpServletRequest.class);
      HttpServletResponse mockResponse = mock(HttpServletResponse.class);
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      when(mockResponse.getWriter()).thenReturn(printWriter);
      when(mockRequest.getRequestURI()).thenReturn(path);
      when(mockRequest.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
          .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
      when(authenticator.supportsToken(any())).thenReturn(true);
      when(authenticator.isDataFromToken()).thenReturn(true);
      when(authenticator.authenticateToken(any()))
          .thenThrow(new UnauthorizedException("UNAUTHORIZED"));

      filter.doFilter(mockRequest, mockResponse, mockChain);

      // Auth flow ran and rejected — proves these paths are not exempted.
      verify(mockResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      verify(mockResponse).setContentType("application/json");
    }
  }

  @Test
  public void testUnauthorizedErrorReturnsJsonBody() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList());

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(
        response, new UnauthorizedException("The provided credentials did not support"));

    verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");

    printWriter.flush();
    String json = stringWriter.toString();
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();
    ErrorResponse errorResponse = mapper.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(1011, errorResponse.getCode());
    Assertions.assertEquals("UnauthorizedException", errorResponse.getType());
    Assertions.assertEquals("The provided credentials did not support", errorResponse.getMessage());
  }

  @Test
  public void testForbiddenErrorReturnsJsonBody() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList());

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new ForbiddenException("Access denied"));

    verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");

    printWriter.flush();
    String json = stringWriter.toString();
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();
    ErrorResponse errorResponse = mapper.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(1008, errorResponse.getCode());
    Assertions.assertEquals("ForbiddenException", errorResponse.getType());
    Assertions.assertEquals("Access denied", errorResponse.getMessage());
  }

  @Test
  public void testInternalServerErrorReturnsJsonBody() throws Exception {
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList());

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new RuntimeException("Something went wrong"));

    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    verify(response).setContentType("application/json");
    verify(response).setCharacterEncoding("UTF-8");

    printWriter.flush();
    String json = stringWriter.toString();
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();
    ErrorResponse errorResponse = mapper.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(1002, errorResponse.getCode());
    Assertions.assertEquals("RuntimeException", errorResponse.getType());
    Assertions.assertEquals("Something went wrong", errorResponse.getMessage());
  }

  @Test
  public void testUnexpectedAuthenticationErrorIsLogged() throws Exception {
    RuntimeException failure = new RuntimeException("authenticator bug");
    Authenticator authenticator = mock(Authenticator.class);
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenThrow(failure);
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain mockChain = mock(FilterChain.class);
    HttpServletRequest mockRequest = requestWithAuthorizationHeader();
    HttpServletResponse mockResponse = responseWithWriter();

    List<LogEvent> errors =
        captureErrorLogs(() -> filter.doFilter(mockRequest, mockResponse, mockChain));

    verify(mockResponse).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    verify(mockChain, never()).doFilter(any(), any());
    Assertions.assertEquals(1, errors.size());
    Assertions.assertSame(failure, errors.get(0).getThrown());
  }

  @Test
  public void testClientAuthenticationErrorsAreNotLogged() throws Exception {
    Authenticator rejectingAuthenticator = mock(Authenticator.class);
    when(rejectingAuthenticator.supportsToken(any())).thenReturn(true);
    when(rejectingAuthenticator.isDataFromToken()).thenReturn(true);
    when(rejectingAuthenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("UNAUTHORIZED"));
    AuthenticationFilter rejectingFilter =
        new AuthenticationFilter(Lists.newArrayList(rejectingAuthenticator));
    HttpServletResponse unauthorizedResponse = responseWithWriter();

    Authenticator acceptingAuthenticator = mock(Authenticator.class);
    when(acceptingAuthenticator.supportsToken(any())).thenReturn(true);
    when(acceptingAuthenticator.isDataFromToken()).thenReturn(true);
    when(acceptingAuthenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    AuthenticationFilter acceptingFilter =
        new AuthenticationFilter(Lists.newArrayList(acceptingAuthenticator));
    HttpServletRequest malformedRolesRequest = requestWithAuthorizationHeader();
    when(malformedRolesRequest.getHeader(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER))
        .thenReturn("ALL,analyst");
    HttpServletResponse badRequestResponse = responseWithWriter();

    Authenticator forbiddingAuthenticator = mock(Authenticator.class);
    when(forbiddingAuthenticator.supportsToken(any())).thenReturn(true);
    when(forbiddingAuthenticator.isDataFromToken()).thenReturn(true);
    when(forbiddingAuthenticator.authenticateToken(any()))
        .thenThrow(new ForbiddenException("Access denied"));
    AuthenticationFilter forbiddingFilter =
        new AuthenticationFilter(Lists.newArrayList(forbiddingAuthenticator));
    HttpServletResponse forbiddenResponse = responseWithWriter();

    List<LogEvent> errors =
        captureErrorLogs(
            () -> {
              rejectingFilter.doFilter(
                  requestWithAuthorizationHeader(), unauthorizedResponse, mock(FilterChain.class));
              acceptingFilter.doFilter(
                  malformedRolesRequest, badRequestResponse, mock(FilterChain.class));
              forbiddingFilter.doFilter(
                  requestWithAuthorizationHeader(), forbiddenResponse, mock(FilterChain.class));
            });

    verify(unauthorizedResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(badRequestResponse).setStatus(HttpServletResponse.SC_BAD_REQUEST);
    verify(forbiddenResponse).setStatus(HttpServletResponse.SC_FORBIDDEN);
    Assertions.assertTrue(errors.isEmpty());
  }

  @Test
  public void testDownstreamFailureIsNotLoggedByFilter() throws Exception {
    Authenticator authenticator = mock(Authenticator.class);
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    FilterChain failingChain =
        (req, resp) -> {
          throw new ServletException("downstream failure");
        };
    HttpServletResponse mockResponse = responseWithWriter();

    List<LogEvent> errors =
        captureErrorLogs(
            () -> filter.doFilter(requestWithAuthorizationHeader(), mockResponse, failingChain));

    verify(mockResponse).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertTrue(errors.isEmpty());
  }

  @Test
  public void testUnauthorizedResponseSetsOnlyNonBasicChallenges() throws Exception {
    Authenticator negotiateAuthenticator = mock(Authenticator.class);
    when(negotiateAuthenticator.supportsToken(any())).thenReturn(true);
    when(negotiateAuthenticator.isDataFromToken()).thenReturn(true);
    when(negotiateAuthenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("Blank token found", AuthConstants.NEGOTIATE));
    HttpServletResponse negotiateResponse = responseWithWriter();

    Authenticator basicAuthenticator = mock(Authenticator.class);
    when(basicAuthenticator.supportsToken(any())).thenReturn(true);
    when(basicAuthenticator.isDataFromToken()).thenReturn(true);
    when(basicAuthenticator.authenticateToken(any()))
        .thenThrow(new UnauthorizedException("Bad credentials", "Basic realm=\"gravitino\""));
    HttpServletResponse basicResponse = responseWithWriter();

    new AuthenticationFilter(Lists.newArrayList(negotiateAuthenticator))
        .doFilter(requestWithAuthorizationHeader(), negotiateResponse, mock(FilterChain.class));
    new AuthenticationFilter(Lists.newArrayList(basicAuthenticator))
        .doFilter(requestWithAuthorizationHeader(), basicResponse, mock(FilterChain.class));

    verify(negotiateResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(negotiateResponse)
        .setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, AuthConstants.NEGOTIATE);
    verify(basicResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(basicResponse, never()).setHeader(eq(AuthConstants.HTTP_CHALLENGE_HEADER), anyString());
  }

  @Test
  public void testDownstreamUnauthorizedReturns401WithChallenge() throws Exception {
    Authenticator authenticator = mock(Authenticator.class);
    when(authenticator.supportsToken(any())).thenReturn(true);
    when(authenticator.isDataFromToken()).thenReturn(true);
    when(authenticator.authenticateToken(any())).thenReturn(new UserPrincipal("user"));
    AuthenticationFilter filter = new AuthenticationFilter(Lists.newArrayList(authenticator));
    HttpServletRequest mockRequest = requestWithAuthorizationHeader();
    HttpServletResponse mockResponse = responseWithWriter();
    FilterChain rejectingChain =
        (req, resp) -> {
          throw new UnauthorizedException("Token expired downstream", AuthConstants.NEGOTIATE);
        };

    List<LogEvent> errors =
        captureErrorLogs(() -> filter.doFilter(mockRequest, mockResponse, rejectingChain));

    verify(mockRequest)
        .setAttribute(eq(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME), any());
    verify(mockResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    verify(mockResponse).setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, AuthConstants.NEGOTIATE);
    Assertions.assertTrue(errors.isEmpty());
  }

  private static HttpServletRequest requestWithAuthorizationHeader() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION))
        .thenReturn(new Vector<>(Collections.singletonList("user")).elements());
    return request;
  }

  private static HttpServletResponse responseWithWriter() throws IOException {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.getWriter()).thenReturn(new PrintWriter(new StringWriter()));
    return response;
  }

  private interface FilterInvocation {
    void run() throws Exception;
  }

  /** Runs the invocation and returns the ERROR events logged by {@link AuthenticationFilter}. */
  private static List<LogEvent> captureErrorLogs(FilterInvocation invocation) throws Exception {
    String loggerName = AuthenticationFilter.class.getName();
    LoggerContext loggerContext =
        (LoggerContext) LogManager.getContext(AuthenticationFilter.class.getClassLoader(), false);
    AbstractConfiguration configuration = (AbstractConfiguration) loggerContext.getConfiguration();
    LoggerConfig previousLoggerConfig = configuration.getLoggers().get(loggerName);
    List<LogEvent> events = new CopyOnWriteArrayList<>();
    AbstractAppender appender =
        new AbstractAppender(
            "authenticationFilterCapture", null, PatternLayout.createDefaultLayout(), true, null) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    try {
      appender.start();
      configuration.addAppender(appender);
      LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.ERROR, false);
      loggerConfig.addAppender(appender, Level.ERROR, null);
      configuration.addLogger(loggerName, loggerConfig);
      loggerContext.updateLoggers();
      invocation.run();
    } finally {
      configuration.removeLogger(loggerName);
      if (previousLoggerConfig != null) {
        configuration.addLogger(loggerName, previousLoggerConfig);
      }
      configuration.removeAppender(appender.getName());
      appender.stop();
      loggerContext.updateLoggers();
    }
    return events;
  }
}
