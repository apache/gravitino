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
import static org.mockito.Mockito.when;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestLanceAuthenticationFilter {

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
        "/health",
        "/health/live",
        "/health.html",
        "/api/health",
        "/api/health/live"
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
}
