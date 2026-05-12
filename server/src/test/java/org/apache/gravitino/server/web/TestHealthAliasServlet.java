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
package org.apache.gravitino.server.web;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestHealthAliasServlet {

  @ParameterizedTest
  @CsvSource({
    "/health,        /api/health",
    "/health.html,   /api/health",
    "/health/live,   /api/health/live",
    "/health/ready,  /api/health/ready"
  })
  public void testDoGetForwardsToCanonicalPath(String incoming, String expected) throws Exception {
    HealthAliasServlet servlet = new HealthAliasServlet();
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    RequestDispatcher dispatcher = mock(RequestDispatcher.class);
    when(req.getRequestURI()).thenReturn(incoming.strip());
    when(req.getRequestDispatcher(expected.strip())).thenReturn(dispatcher);

    servlet.doGet(req, resp);

    verify(dispatcher).forward(req, resp);
  }

  @Test
  public void testDoGetReturns503WhenDispatcherIsNull() throws Exception {
    HealthAliasServlet servlet = new HealthAliasServlet();
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getRequestURI()).thenReturn("/health");
    when(req.getRequestDispatcher("/api/health")).thenReturn(null);

    servlet.doGet(req, resp);

    verify(resp)
        .sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "health dispatcher unavailable");
  }
}
