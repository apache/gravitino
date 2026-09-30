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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.utils.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRequestContextFilter {

  // No EventBus: exercises the remote-address-only path shared by every request.
  private final RequestContextFilter filter = new RequestContextFilter();
  // With an EventBus configured: exercises query-parameter capture, which is otherwise skipped.
  private final RequestContextFilter filterWithEventBus =
      new RequestContextFilter(new EventBus(Collections.emptyList()));

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

  /**
   * Query parameters are captured raw here, not redacted — redaction happens once, uniformly, at
   * audit-log format time (see AuditLogRedactor's class doc). So a sensitive-looking parameter like
   * "token" must come through as its real value at this layer, not already masked.
   */
  @Test
  public void testQueryParamsCapturedRaw() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("details=true&token=secret-value");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    filterWithEventBus.doFilter(req, resp, chain);

    Assertions.assertEquals("true", captured.get().get("details"));
    Assertions.assertEquals("secret-value", captured.get().get("token"));
  }

  @Test
  public void testMultiValuedQueryParamIsJoinedWithComma() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("keyword=a&keyword=b&keyword=c");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    filterWithEventBus.doFilter(req, resp, chain);

    Assertions.assertEquals("a,b,c", captured.get().get("keyword"));
  }

  /**
   * Query strings are percent-encoded ({@code application/x-www-form-urlencoded}); both the name
   * and the value must come back decoded, including {@code +} decoding to a space.
   */
  @Test
  public void testQueryParamsAreUrlDecoded() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("full+name=Alice+Smith&sym=%26");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    filterWithEventBus.doFilter(req, resp, chain);

    Assertions.assertEquals("Alice Smith", captured.get().get("full name"));
    Assertions.assertEquals("&", captured.get().get("sym"));
  }

  /**
   * A name with no {@code =} (e.g. {@code ?flag}) is a valid query string; it must be captured with
   * an empty value rather than crashing the whole request.
   */
  @Test
  public void testValuelessParamDoesNotThrow() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("flag");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    Assertions.assertDoesNotThrow(() -> filterWithEventBus.doFilter(req, resp, chain));
    Assertions.assertEquals("", captured.get().get("flag"));
  }

  /**
   * The query string is attacker-influenced input; malformed percent-encoding (e.g. a truncated
   * {@code %} escape) must not crash the whole request — it falls back to the raw component.
   */
  @Test
  public void testMalformedPercentEncodingDoesNotThrow() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("bad=100%");

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    Assertions.assertDoesNotThrow(() -> filterWithEventBus.doFilter(req, resp, chain));
    Assertions.assertEquals("100%", captured.get().get("bad"));
  }

  /**
   * Pins the bound on parameter count: a query string with more distinct names than {@link
   * RequestContextFilter#MAX_PARAMETERS} must not grow the captured map without limit.
   */
  @Test
  public void testParameterCountIsCapped() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    String queryString =
        IntStream.range(0, RequestContextFilter.MAX_PARAMETERS + 20)
            .mapToObj(i -> "p" + i + "=v")
            .collect(Collectors.joining("&"));
    when(req.getQueryString()).thenReturn(queryString);

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    filterWithEventBus.doFilter(req, resp, chain);

    Assertions.assertEquals(RequestContextFilter.MAX_PARAMETERS, captured.get().size());
  }

  /**
   * Pins the bound on value length: a single oversized value must be truncated with an explicit
   * marker, not copied in full into every event constructed during the request.
   */
  @Test
  public void testValueLengthIsTruncated() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    String hugeValue =
        String.join("", Collections.nCopies(RequestContextFilter.MAX_VALUE_LENGTH + 100, "a"));
    when(req.getQueryString()).thenReturn("big=" + hugeValue);

    AtomicReference<Map<String, String>> captured = new AtomicReference<>();
    FilterChain chain = (request, response) -> captured.set(RequestContext.getRequestQueryParams());

    filterWithEventBus.doFilter(req, resp, chain);

    String capturedBig = captured.get().get("big");
    Assertions.assertTrue(capturedBig.endsWith(RequestContextFilter.TRUNCATED_SUFFIX), capturedBig);
    Assertions.assertEquals(
        RequestContextFilter.MAX_VALUE_LENGTH + RequestContextFilter.TRUNCATED_SUFFIX.length(),
        capturedBig.length());
  }

  @Test
  public void testQueryParamsClearedAfterChain() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("details=true");

    filterWithEventBus.doFilter(req, resp, (request, response) -> {});

    Assertions.assertTrue(
        RequestContext.getRequestQueryParams().isEmpty(),
        "query-param ThreadLocal must be cleared after chain completes");
  }

  /**
   * Pins the efficiency fix: when no EventBus is configured, nothing will ever read the captured
   * query parameters, so capture (and its redaction-list scanning) is skipped entirely — remote
   * address is still captured, since HttpAuditFilter's own fallback events need it regardless of
   * whether any listener is configured.
   */
  @Test
  public void testQueryParamsNotCapturedWithoutEventBus() throws IOException, ServletException {
    HttpServletRequest req = mock(HttpServletRequest.class);
    HttpServletResponse resp = mock(HttpServletResponse.class);
    when(req.getHeader("X-Forwarded-For")).thenReturn(null);
    when(req.getRemoteAddr()).thenReturn("1.2.3.4");
    when(req.getQueryString()).thenReturn("details=true");

    AtomicReference<Map<String, String>> capturedParams = new AtomicReference<>();
    AtomicReference<String> capturedAddress = new AtomicReference<>();
    FilterChain chain =
        (request, response) -> {
          capturedParams.set(RequestContext.getRequestQueryParams());
          capturedAddress.set(RequestContext.getRemoteAddress());
        };

    filter.doFilter(req, resp, chain);

    Assertions.assertTrue(capturedParams.get().isEmpty());
    Assertions.assertEquals("1.2.3.4", capturedAddress.get());
  }
}
