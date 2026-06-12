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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.Principal;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestHttpAuditFilter {

  @AfterEach
  public void cleanup() {
    RequestContext.resetOperationFailureFired();
    RequestContext.clear();
  }

  // ─── Null EventBus ───────────────────────────────────────────────────────────

  @Test
  public void testNullEventBusPassThrough() throws Exception {
    HttpAuditFilter filter = new HttpAuditFilter(null, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "1.2.3.4");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(req, resp, chain);

    verify(chain).doFilter(req, resp);
  }

  // ─── Non-HTTP request ────────────────────────────────────────────────────────

  @Test
  public void testNonHttpRequestPassThrough() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    ServletRequest req = mock(ServletRequest.class);
    ServletResponse resp = mock(ServletResponse.class);
    FilterChain chain = mock(FilterChain.class);

    filter.doFilter(req, resp, chain);

    verify(chain).doFilter(req, resp);
    verify(eventBus, never()).dispatchEvent(any());
  }

  // ─── Health check bypass ─────────────────────────────────────────────────────

  @Test
  public void testHealthCheckPathsSkipped() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);

    for (String path :
        new String[] {
          "/health", "/health/live", "/health.html", "/api/health", "/api/health/ready"
        }) {
      HttpServletRequest req = mockRequest("GET", path, null, "1.2.3.4");
      HttpServletResponse resp = mock(HttpServletResponse.class);
      FilterChain chain = mock(FilterChain.class);

      filter.doFilter(req, resp, chain);

      verify(chain).doFilter(req, resp);
    }

    verify(eventBus, never()).dispatchEvent(any());
  }

  // ─── Successful 200 — no event ───────────────────────────────────────────────

  @Test
  public void test200ResponseNoEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "1.2.3.4");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).setStatus(200);

    filter.doFilter(req, resp, chain);

    verify(eventBus, never()).dispatchEvent(any());
  }

  // ─── 4xx via sendError ───────────────────────────────────────────────────────

  @Test
  public void test401SendErrorEmitsEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("POST", "/api/metalakes", null, "10.0.0.1");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> ((HttpServletResponse) response).sendError(401, "Unauthorized");

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    Assertions.assertInstanceOf(HttpRequestFailureEvent.class, captor.getValue());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals(401, event.statusCode());
    Assertions.assertEquals("POST", event.httpMethod());
    Assertions.assertEquals("/api/metalakes", event.requestUri());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, event.eventSource());
  }

  @Test
  public void test404SetStatusEmitsEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/unknown-route", null, "2.2.2.2");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).setStatus(404);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals(404, event.statusCode());
    Assertions.assertEquals("GET", event.httpMethod());
  }

  // ─── Double-logging prevention ───────────────────────────────────────────────

  @Test
  public void testOperationFailureFiredFlagPreventsEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("DELETE", "/api/metalakes/m1", null, "3.3.3.3");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    // Simulate an operation-layer FailureEvent having already been dispatched
    FilterChain chain =
        (request, response) -> {
          RequestContext.markOperationFailureFired();
          ((HttpServletResponse) response).sendError(403, "Forbidden");
        };

    filter.doFilter(req, resp, chain);

    verify(eventBus, never()).dispatchEvent(any());
  }

  // ─── Exception escape ────────────────────────────────────────────────────────

  @Test
  public void testRuntimeExceptionEscapePromotesTo500AndEmitsEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "5.5.5.5");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> {
          throw new RuntimeException("simulated crash");
        };

    Assertions.assertThrows(RuntimeException.class, () -> filter.doFilter(req, resp, chain));

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals(500, event.statusCode());
  }

  @Test
  public void testServletExceptionEscapeIsRethrown() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "5.5.5.5");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> {
          throw new ServletException("simulated servlet error");
        };

    Assertions.assertThrows(ServletException.class, () -> filter.doFilter(req, resp, chain));
  }

  @Test
  public void testIoExceptionEscapeIsRethrown() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "5.5.5.5");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> {
          throw new IOException("simulated IO error");
        };

    Assertions.assertThrows(IOException.class, () -> filter.doFilter(req, resp, chain));
  }

  // ─── ThreadLocal cleanup ─────────────────────────────────────────────────────

  @Test
  public void testFlagClearedAfterNormalCompletion() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "1.1.1.1");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> {
          RequestContext.markOperationFailureFired();
          ((HttpServletResponse) response).sendError(403);
        };

    filter.doFilter(req, resp, chain);

    Assertions.assertFalse(
        RequestContext.isOperationFailureFired(),
        "operationFailureFired flag must be cleared after filter completes");
  }

  @Test
  public void testFlagClearedAfterExceptionEscape() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "1.1.1.1");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> {
          RequestContext.markOperationFailureFired();
          throw new RuntimeException("crash with flag set");
        };

    Assertions.assertThrows(RuntimeException.class, () -> filter.doFilter(req, resp, chain));

    Assertions.assertFalse(
        RequestContext.isOperationFailureFired(),
        "operationFailureFired flag must be cleared even when chain throws");
  }

  // ─── Remote address resolution ───────────────────────────────────────────────

  @Test
  public void testXForwardedForFirstEntryUsedAsRemoteAddress() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req =
        mockRequest("GET", "/api/metalakes", "203.0.113.5, 10.1.1.1, 10.2.2.2", "10.0.0.1");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(401);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals("203.0.113.5", event.remoteAddress());
  }

  @Test
  public void testRawRemoteAddrUsedWhenNoXForwardedFor() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "192.168.0.42");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(401);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals("192.168.0.42", event.remoteAddress());
  }

  @Test
  public void testAuthenticatedPrincipalAttributeUsedForUser() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("POST", "/api/metalakes", null, "10.0.0.1");
    Principal principal = () -> "alice";
    when(req.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME))
        .thenReturn(principal);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain =
        (request, response) -> ((HttpServletResponse) response).sendError(400, "bad request");

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals("alice", event.user());
  }

  @Test
  public void testNoAuthAttributeResolvesToUnknown() throws Exception {
    // When the auth filter rejects a request (e.g. wrong token scheme → 401), it never calls
    // request.setAttribute(AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, ...). The filter must report
    // "unknown", not "anonymous" (which PrincipalUtils.getCurrentUserName() would return as its
    // no-Subject fallback and is reserved for explicitly-anonymous authenticated requests).
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "10.0.0.1");
    when(req.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME)).thenReturn(null);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(401);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals("unknown", event.user());
  }

  // ─── Success + 5xx edge case ─────────────────────────────────────────────────

  @Test
  public void testSuccessEventFollowedBy5xxStillEmitsHttpFailureEvent() throws Exception {
    // An operation dispatcher emits a SuccessEvent (operationFailureFired stays false),
    // but the HTTP response ends up as 500 (e.g. JSON serialization failure).
    // HttpAuditFilter must still emit HttpRequestFailureEvent for the HTTP-layer failure.
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes/m1", null, "1.2.3.4");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    // operationFailureFired is NOT set (success event path)
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(500);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals(500, event.statusCode());
  }

  @Test
  public void testStaleOperationFailureFlagDoesNotSuppressCurrentRequestEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter = new HttpAuditFilter(eventBus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest req = mockRequest("GET", "/api/metalakes", null, "1.1.1.1");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(401);
    // Simulate leaked state from a previous request on a pooled thread.
    RequestContext.markOperationFailureFired();

    filter.doFilter(req, resp, chain);

    verify(eventBus).dispatchEvent(any());
  }

  // ─── EventSource propagation ─────────────────────────────────────────────────

  @Test
  public void testEventSourcePropagatedToEvent() throws Exception {
    EventBus eventBus = mock(EventBus.class);
    HttpAuditFilter filter =
        new HttpAuditFilter(eventBus, EventSource.GRAVITINO_ICEBERG_REST_SERVER);
    HttpServletRequest req = mockRequest("GET", "/iceberg/namespaces", null, "1.2.3.4");
    HttpServletResponse resp = mock(HttpServletResponse.class);
    FilterChain chain = (request, response) -> ((HttpServletResponse) response).sendError(404);

    filter.doFilter(req, resp, chain);

    ArgumentCaptor<BaseEvent> captor = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus).dispatchEvent(captor.capture());
    HttpRequestFailureEvent event = (HttpRequestFailureEvent) captor.getValue();
    Assertions.assertEquals(EventSource.GRAVITINO_ICEBERG_REST_SERVER, event.eventSource());
  }

  // ─── StatusCapturingResponseWrapper ─────────────────────────────────────────

  @Test
  public void testStatusCapturingWrapperDefaultIs200() {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    HttpAuditFilter.StatusCapturingResponseWrapper wrapper =
        new HttpAuditFilter.StatusCapturingResponseWrapper(resp);

    Assertions.assertEquals(200, wrapper.getCapturedStatus());
  }

  @Test
  public void testStatusCapturingWrapperSetStatus() {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    HttpAuditFilter.StatusCapturingResponseWrapper wrapper =
        new HttpAuditFilter.StatusCapturingResponseWrapper(resp);

    wrapper.setStatus(404);

    Assertions.assertEquals(404, wrapper.getCapturedStatus());
  }

  @Test
  public void testStatusCapturingWrapperSendErrorWithMessage() throws IOException {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    HttpAuditFilter.StatusCapturingResponseWrapper wrapper =
        new HttpAuditFilter.StatusCapturingResponseWrapper(resp);

    wrapper.sendError(401, "Unauthorized");

    Assertions.assertEquals(401, wrapper.getCapturedStatus());
  }

  @Test
  public void testStatusCapturingWrapperSendErrorWithoutMessage() throws IOException {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    HttpAuditFilter.StatusCapturingResponseWrapper wrapper =
        new HttpAuditFilter.StatusCapturingResponseWrapper(resp);

    wrapper.sendError(403);

    Assertions.assertEquals(403, wrapper.getCapturedStatus());
  }

  @Test
  public void testStatusCapturingWrapperResetRestoresTo200() throws IOException {
    HttpServletResponse resp = mock(HttpServletResponse.class);
    HttpAuditFilter.StatusCapturingResponseWrapper wrapper =
        new HttpAuditFilter.StatusCapturingResponseWrapper(resp);

    wrapper.sendError(500, "Internal Server Error");
    Assertions.assertEquals(500, wrapper.getCapturedStatus());

    wrapper.reset();
    Assertions.assertEquals(200, wrapper.getCapturedStatus(), "reset() must restore status to 200");
  }

  // ─── Helper ──────────────────────────────────────────────────────────────────

  private HttpServletRequest mockRequest(
      String method, String uri, String xForwardedFor, String remoteAddr) {
    HttpServletRequest req = mock(HttpServletRequest.class);
    when(req.getMethod()).thenReturn(method);
    when(req.getRequestURI()).thenReturn(uri);
    when(req.getHeader("X-Forwarded-For")).thenReturn(xForwardedFor);
    when(req.getRemoteAddr()).thenReturn(remoteAddr);
    return req;
  }
}
