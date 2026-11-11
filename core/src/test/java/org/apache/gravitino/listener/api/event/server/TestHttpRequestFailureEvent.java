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

package org.apache.gravitino.listener.api.event.server;

import java.util.Map;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHttpRequestFailureEvent {

  @Test
  public void testFieldsStoredCorrectly() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", "203.0.113.5", "POST", "/api/metalakes", 401, EventSource.GRAVITINO_SERVER);

    Assertions.assertEquals("alice", event.user());
    Assertions.assertEquals("203.0.113.5", event.remoteAddress());
    Assertions.assertEquals("POST", event.httpMethod());
    Assertions.assertEquals("/api/metalakes", event.requestUri());
    Assertions.assertEquals(401, event.statusCode());
    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, event.eventSource());
  }

  @Test
  public void testOperationTypeIsAlwaysUnknown() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", "1.2.3.4", "GET", "/api/metalakes", 404, EventSource.GRAVITINO_SERVER);

    Assertions.assertEquals(OperationType.UNKNOWN, event.operationType());
  }

  @Test
  public void testIdentifierIsNull() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", "1.2.3.4", "GET", "/unknown", 404, EventSource.GRAVITINO_SERVER);

    Assertions.assertNull(event.identifier());
  }

  @Test
  public void testCustomInfoContainsHttpFields() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "bob", "10.0.0.1", "DELETE", "/api/metalakes/m1", 403, EventSource.GRAVITINO_SERVER);

    Map<String, String> info = event.customInfo();
    Assertions.assertEquals("DELETE", info.get("http.method"));
    Assertions.assertEquals("/api/metalakes/m1", info.get("http.uri"));
    Assertions.assertEquals("403", info.get("http.status"));
  }

  @Test
  public void testNullRemoteAddressFallsBackToUnknown() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", null, "GET", "/api/metalakes", 401, EventSource.GRAVITINO_SERVER);

    Assertions.assertEquals("unknown", event.remoteAddress());
  }

  @Test
  public void testExceptionMessageContainsHttpContext() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", "1.2.3.4", "POST", "/api/metalakes", 400, EventSource.GRAVITINO_SERVER);

    String message = event.exception().getMessage();
    Assertions.assertTrue(message.contains("POST"), "Exception message must contain HTTP method");
    Assertions.assertTrue(
        message.contains("/api/metalakes"), "Exception message must contain request URI");
    Assertions.assertTrue(message.contains("400"), "Exception message must contain status code");
  }

  @Test
  public void testExceptionHasNoStackTrace() {
    HttpRequestFailureEvent event =
        new HttpRequestFailureEvent(
            "alice", "1.2.3.4", "GET", "/api", 404, EventSource.GRAVITINO_SERVER);

    Assertions.assertEquals(
        0,
        event.exception().getStackTrace().length,
        "HttpRequestException must suppress stack trace to avoid overhead");
  }

  @Test
  public void testEventSourcePropagated() {
    HttpRequestFailureEvent icebergEvent =
        new HttpRequestFailureEvent(
            "alice",
            "1.2.3.4",
            "GET",
            "/iceberg/namespaces",
            404,
            EventSource.GRAVITINO_ICEBERG_REST_SERVER);

    Assertions.assertEquals(EventSource.GRAVITINO_ICEBERG_REST_SERVER, icebergEvent.eventSource());

    HttpRequestFailureEvent lanceEvent =
        new HttpRequestFailureEvent(
            "alice",
            "1.2.3.4",
            "GET",
            "/lance/namespaces",
            404,
            EventSource.GRAVITINO_LANCE_REST_SERVER);

    Assertions.assertEquals(EventSource.GRAVITINO_LANCE_REST_SERVER, lanceEvent.eventSource());
  }
}
