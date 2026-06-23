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
package org.apache.gravitino.lance.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.lance.namespace.model.ErrorResponse;

public class TestLanceAuthenticationFilter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Exposes the protected {@code isHealthCheckRequest} method for white-box testing. */
  private static class TestableFilter extends LanceAuthenticationFilter {
    boolean isHealth(ServletRequest request) {
      return isHealthCheckRequest(request);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/lance/health",
        "/lance/health/live",
        "/lance/health/ready",
        "/health",
        "/health/live",
        "/health/ready",
        "/health.html",
        "/api/health",
        "/api/health/live",
        "/api/health/ready"
      })
  public void testHealthPathsBypassAuth(String path) {
    TestableFilter filter = new TestableFilter();
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn(path);
    Assertions.assertTrue(filter.isHealth(req), "Expected health bypass for path: " + path);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/lance/v1/namespace",
        "/lance/v1/table",
        "/lance/healthcheck",
        "/iceberg/health",
        "/iceberg/v1/namespaces"
      })
  public void testNonHealthPathsRequireAuth(String path) {
    TestableFilter filter = new TestableFilter();
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getRequestURI()).thenReturn(path);
    Assertions.assertFalse(filter.isHealth(req), "Expected auth required for path: " + path);
  }

  @Test
  public void testNonHttpRequestReturnsFalse() {
    TestableFilter filter = new TestableFilter();
    ServletRequest nonHttpRequest = mock(ServletRequest.class);
    Assertions.assertFalse(filter.isHealth(nonHttpRequest));
  }

  @Test
  public void testUnauthorizedErrorReturnsJson() throws Exception {
    LanceAuthenticationFilter filter = new LanceAuthenticationFilter();

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
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(401, errorResponse.getCode());
    Assertions.assertEquals("The provided credentials did not support", errorResponse.getError());
  }

  @Test
  public void testInternalServerErrorReturnsJson() throws Exception {
    LanceAuthenticationFilter filter = new LanceAuthenticationFilter();

    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    when(response.getWriter()).thenReturn(printWriter);

    filter.sendAuthErrorResponse(response, new RuntimeException("Something went wrong"));

    verify(response).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);

    printWriter.flush();
    String json = stringWriter.toString();
    ErrorResponse errorResponse = MAPPER.readValue(json, ErrorResponse.class);
    Assertions.assertEquals(500, errorResponse.getCode());
    Assertions.assertEquals("Authentication failed", errorResponse.getError());
  }
}
