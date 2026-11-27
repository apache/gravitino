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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.servlet.DispatcherType;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

/** Exercises request header propagation across the context and HTTP audit filter chain. */
public class TestRequestHeaderContext {

  private static final String KEY = RequestContext.REQUEST_HEADER_PREFIX + "x-job-id";

  @AfterEach
  void cleanup() {
    RequestContext.clear();
    MDC.clear();
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 401, 403, 500})
  void testAuditFallbackSnapshotsHeadersForAllStatuses(int status) throws Exception {
    EventBus bus = mock(EventBus.class);
    RequestContextFilter context = filter(bus);
    HttpAuditFilter audit = new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER);
    HttpServletRequest request = request("job-123");
    HttpServletResponse response = mock(HttpServletResponse.class);
    doAnswer(
            invocation -> {
              assertEquals("job-123", MDC.get(KEY));
              return null;
            })
        .when(bus)
        .dispatchEvent(any());
    context.doFilter(
        request,
        response,
        (req, resp) ->
            audit.doFilter(
                req,
                resp,
                (innerReq, innerResp) -> {
                  assertEquals("job-123", MDC.get(KEY));
                  ((HttpServletResponse) innerResp).setStatus(status);
                }));
    ArgumentCaptor<Event> event = ArgumentCaptor.forClass(Event.class);
    verify(bus).dispatchEvent(event.capture());
    assertEquals("job-123", event.getValue().customInfo().get(KEY));
    assertEquals(String.valueOf(status), event.getValue().customInfo().get("http.status"));
    assertTrue(RequestContext.getRequestHeaders().isEmpty());
    assertNull(MDC.get(KEY));
  }

  @Test
  void testEscapedFailureIsAuditedBeforeCleanup() throws Exception {
    EventBus bus = mock(EventBus.class);
    HttpAuditFilter audit = new HttpAuditFilter(bus, EventSource.GRAVITINO_SERVER);
    assertThrows(
        ServletException.class,
        () ->
            filter(bus)
                .doFilter(
                    request("failed-job"),
                    mock(HttpServletResponse.class),
                    (req, resp) ->
                        audit.doFilter(
                            req,
                            resp,
                            (innerReq, innerResp) -> {
                              throw new ServletException("failure");
                            })));
    ArgumentCaptor<Event> event = ArgumentCaptor.forClass(Event.class);
    verify(bus).dispatchEvent(event.capture());
    assertEquals("failed-job", event.getValue().customInfo().get(KEY));
    assertEquals("500", event.getValue().customInfo().get("http.status"));
    assertTrue(RequestContext.getRequestHeaders().isEmpty());
    assertNull(MDC.get(KEY));
  }

  @Test
  void testLoggingWithoutAuditRestoresOnlyOwnedMdcKeys() throws Exception {
    MDC.put(KEY, "outer");
    MDC.put("unrelated", "keep");
    RequestContextFilter filter = filter(null);
    filter.doFilter(
        request("job-1"),
        mock(HttpServletResponse.class),
        (req, resp) -> {
          assertEquals("job-1", MDC.get(KEY));
          assertEquals("keep", MDC.get("unrelated"));
          MDC.put("downstream", "also-keep");
        });
    assertEquals("outer", MDC.get(KEY));
    assertEquals("keep", MDC.get("unrelated"));
    assertEquals("also-keep", MDC.get("downstream"));
    // The next request on this thread has no header; the outer MDC value must not leak into it.
    filter.doFilter(
        request(null),
        mock(HttpServletResponse.class),
        (req, resp) -> {
          assertNull(MDC.get(KEY));
          assertTrue(RequestContext.getRequestHeaders().isEmpty());
        });
    assertEquals("outer", MDC.get(KEY));
  }

  @Test
  void testConcurrentRequestsAndThreadReuse() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    CyclicBarrier barrier = new CyclicBarrier(2);
    RequestContextFilter filter = filter(null);
    try {
      Future<?> first = pool.submit(() -> exerciseThread(filter, "job-1", barrier));
      Future<?> second = pool.submit(() -> exerciseThread(filter, "job-2", barrier));
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
  }

  private void exerciseThread(RequestContextFilter filter, String job, CyclicBarrier barrier) {
    try {
      filter.doFilter(
          request(job),
          mock(HttpServletResponse.class),
          (req, resp) -> {
            try {
              barrier.await(5, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new ServletException(e);
            }
            assertEquals(job, MDC.get(KEY));
            assertEquals(
                Collections.singletonMap("x-job-id", job), RequestContext.getRequestHeaders());
          });
      filter.doFilter(
          request(null),
          mock(HttpServletResponse.class),
          (req, resp) -> {
            assertNull(MDC.get(KEY));
            assertTrue(RequestContext.getRequestHeaders().isEmpty());
          });
      assertNull(MDC.get(KEY));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private RequestContextFilter filter(EventBus bus) {
    return new RequestContextFilter(
        bus,
        ClientAddressResolver.noTrustedProxies(),
        new RequestHeaderCapture(Arrays.asList("X-Job-Id", "X-Execution-Id")));
  }

  private HttpServletRequest request(String job) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("x-job-id")).thenReturn(job);
    when(request.getMethod()).thenReturn("GET");
    when(request.getRequestURI()).thenReturn("/api/metalakes");
    when(request.getDispatcherType()).thenReturn(DispatcherType.REQUEST);
    when(request.getRemoteAddr()).thenReturn("127.0.0.1");
    return request;
  }
}
