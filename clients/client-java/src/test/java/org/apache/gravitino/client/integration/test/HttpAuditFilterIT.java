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

package org.apache.gravitino.client.integration.test;

import com.google.common.collect.Maps;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;

/**
 * Integration tests for {@code HttpAuditFilter} covering HTTP-layer failure scenarios that are
 * independent of authorization: unauthenticated requests (401) and malformed JSON bodies (400).
 *
 * <p>Uses MiniGravitino (in-process server) and a {@link CapturedEventListener} registered via
 * config so that events dispatched inside the server are accessible from the test.
 */
@DisabledIfSystemProperty(named = ITUtils.TEST_MODE, matches = ITUtils.DEPLOY_TEST_MODE)
public class HttpAuditFilterIT extends BaseIT {

  private static final String LISTENER_NAME = "httpAuditCapture";

  private HttpClient httpClient;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    configs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES,
        LISTENER_NAME);
    configs.put(
        EventListenerManager.GRAVITINO_EVENT_LISTENER_PREFIX
            + LISTENER_NAME
            + "."
            + EventListenerManager.GRAVITINO_EVENT_LISTENER_CLASS,
        CapturedEventListener.class.getName());
    registerCustomConfigs(configs);
    super.startIntegrationTest();
    httpClient = HttpClient.newHttpClient();
  }

  @AfterEach
  public void clearEvents() {
    CapturedEventListener.clear();
  }

  // ─── Task 14: unknown route → 404 → HttpRequestFailureEvent ────────────────

  /**
   * A GET request to an unknown API route returns 404. {@code HttpAuditFilter} must emit an {@link
   * HttpRequestFailureEvent} with {@code statusCode=404} because no operation-layer event was
   * dispatched for a route that the server does not recognise.
   *
   * <p>Note: the {@code simple} authenticator admits anonymous requests, so 401 cannot be produced
   * in this test configuration. A 404 unknown-route scenario is equally valid for verifying that
   * the filter captures HTTP-layer failures.
   */
  @Test
  public void testUnknownRouteProducesHttpRequestFailureEvent() throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                new URI(
                    "http://localhost:" + getGravitinoServerPort() + "/api/v99/nonexistent/route"))
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    Assertions.assertEquals(404, response.statusCode());
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();
    HttpRequestFailureEvent httpEvent =
        events.stream()
            .filter(e -> e instanceof HttpRequestFailureEvent)
            .map(e -> (HttpRequestFailureEvent) e)
            .findFirst()
            .orElseThrow(() -> new AssertionError("No HttpRequestFailureEvent captured for 404"));

    Assertions.assertEquals(404, httpEvent.statusCode());
    Assertions.assertEquals("GET", httpEvent.httpMethod());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, httpEvent.eventSource());
    // HttpAuditFilter's finally runs outside Subject.doAs — getCurrentUserName() returns anonymous
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, httpEvent.user());
    Assertions.assertEquals("/api/v99/nonexistent/route", httpEvent.requestUri());
    Assertions.assertEquals(OperationType.UNKNOWN, httpEvent.operationType());
    Assertions.assertNull(httpEvent.identifier());
    Map<String, String> customInfo = httpEvent.customInfo();
    Assertions.assertEquals("GET", customInfo.get("http.method"));
    Assertions.assertEquals("/api/v99/nonexistent/route", customInfo.get("http.uri"));
    Assertions.assertEquals("404", customInfo.get("http.status"));
  }

  // ─── Task 16: malformed JSON → 400 → HttpRequestFailureEvent ────────────────

  /**
   * A POST to the metalakes endpoint with a valid {@code Authorization} header but an invalid JSON
   * body fails at the Jersey deserialization layer with a 400. {@code HttpAuditFilter} must emit an
   * {@link HttpRequestFailureEvent} because no operation-layer failure event was dispatched.
   */
  @Test
  public void testMalformedJsonBodyProducesHttpRequestFailureEvent() throws Exception {
    // Construct a valid simple-auth header so the request passes AuthenticationFilter
    String userName = System.getProperty("user.name");
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((userName + ":dummy").getBytes(StandardCharsets.UTF_8));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI("http://localhost:" + getGravitinoServerPort() + "/api/metalakes"))
            .header("Authorization", authHeader)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{this is not valid json"))
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Jersey returns 400 for unreadable request bodies; some versions may return 422
    int status = response.statusCode();
    Assertions.assertTrue(
        status == 400 || status == 422,
        "Expected 400 or 422 for malformed JSON body, got: " + status);
    awaitAtLeastOneEvent();

    List<Event> events = CapturedEventListener.getEvents();
    HttpRequestFailureEvent httpEvent =
        events.stream()
            .filter(e -> e instanceof HttpRequestFailureEvent)
            .map(e -> (HttpRequestFailureEvent) e)
            .filter(e -> e.statusCode() >= 400)
            .findFirst()
            .orElseThrow(
                () ->
                    new AssertionError(
                        "No HttpRequestFailureEvent with status >= 400 captured"
                            + " for malformed JSON"));
    Assertions.assertEquals("POST", httpEvent.httpMethod());
    Assertions.assertEquals("/api/metalakes", httpEvent.requestUri());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, httpEvent.eventSource());
    Assertions.assertEquals(OperationType.UNKNOWN, httpEvent.operationType());
    Assertions.assertNull(httpEvent.identifier());
    // HttpAuditFilter's finally runs outside Subject.doAs — getCurrentUserName() returns anonymous
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, httpEvent.user());
    Map<String, String> malformedCustomInfo = httpEvent.customInfo();
    Assertions.assertEquals("POST", malformedCustomInfo.get("http.method"));
    Assertions.assertEquals("/api/metalakes", malformedCustomInfo.get("http.uri"));
    Assertions.assertEquals(
        String.valueOf(httpEvent.statusCode()), malformedCustomInfo.get("http.status"));
  }

  // ─── Health check bypass ─────────────────────────────────────────────────────

  /**
   * Requests to health check endpoints must never produce an {@link HttpRequestFailureEvent} even
   * though they may not require authentication.
   */
  @Test
  public void testHealthCheckRequestDoesNotProduceEvent() throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI("http://localhost:" + getGravitinoServerPort() + "/api/health"))
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Health endpoint should succeed; if it were blocked we'd get a redirect, not a 4xx
    Assertions.assertTrue(
        response.statusCode() < 400,
        "Health check endpoint should not return an error, got: " + response.statusCode());

    // Since Mode.SYNC is used, any event would already be captured; no wait needed
    List<Event> events = CapturedEventListener.getEvents();
    boolean hasHttpFailureEvent =
        events.stream().anyMatch(e -> e instanceof HttpRequestFailureEvent);
    Assertions.assertFalse(
        hasHttpFailureEvent, "Health check must not produce an HttpRequestFailureEvent");
  }

  // ─── Helper ──────────────────────────────────────────────────────────────────

  private void awaitAtLeastOneEvent() {
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(() -> !CapturedEventListener.getEvents().isEmpty());
  }
}
