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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestContextFilter {

  private final RequestContextFilter filter = new RequestContextFilter();

  @AfterEach
  public void cleanup() {
    RequestContext.clear();
  }

  @Test
  public void testSetsRemoteAddrFromRequest() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("192.168.1.1");

    AtomicReference<String> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRemoteAddress());

    filter.doFilter(req, resp, chain);

    Assertions.assertEquals("192.168.1.1", captured.get());
  }

  @Test
  public void testXForwardedForSingleEntry() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn("203.0.113.5");
    when(req.getRemoteAddr()).thenReturn("10.0.0.1");

    AtomicReference<String> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRemoteAddress());

    filter.doFilter(req, resp, chain);

    Assertions.assertEquals("203.0.113.5", captured.get());
  }

  @Test
  public void testXForwardedForMultipleEntriesUsesFirst() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn("203.0.113.5, 10.1.1.1, 10.2.2.2");
    when(req.getRemoteAddr()).thenReturn("10.0.0.1");

    AtomicReference<String> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRemoteAddress());

    filter.doFilter(req, resp, chain);

    Assertions.assertEquals("203.0.113.5", captured.get());
  }

  @Test
  public void testThreadLocalClearedAfterChain() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");

    filter.doFilter(req, resp, (request, response) -> {});

    Assertions.assertNull(
        RequestContext.getRemoteAddress(), "ThreadLocal must be cleared after chain completes");
  }

  @Test
  public void testThreadLocalClearedEvenOnChainException() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");

    FilterChain throwingChain =
        (request, response) -> {
          throw new ServletException("simulated error");
        };

    Assertions.assertThrows(
        ServletException.class, () -> filter.doFilter(req, resp, throwingChain));
    Assertions.assertNull(
        RequestContext.getRemoteAddress(), "ThreadLocal must be cleared even when chain throws");
  }
}
