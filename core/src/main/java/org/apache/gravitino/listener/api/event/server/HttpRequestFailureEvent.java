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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Represents an HTTP-level request failure that occurred before or outside the operation dispatcher
 * layer — for example, a 401 authentication rejection, a 400 malformed JSON body, or a 404 unknown
 * route. It is emitted by {@code HttpAuditFilter} when the HTTP response status is 4xx or 5xx and
 * no operation-layer {@link FailureEvent} was already dispatched for the same request.
 *
 * <p>Unlike operation-layer failure events, this event carries no {@link
 * org.apache.gravitino.NameIdentifier} (the resource was never resolved) and its {@link
 * #operationType()} is always {@link OperationType#UNKNOWN}. HTTP-specific context (method, URI,
 * status code) is available via {@link #customInfo()}.
 *
 * <p>This event extends {@link FailureEvent} so it is routed through {@code
 * EventBus.dispatchFailureEvent()}, which swallows listener exceptions and prevents audit failures
 * from masking the original HTTP error.
 */
@DeveloperApi
public final class HttpRequestFailureEvent extends FailureEvent {

  private final String explicitRemoteAddress;
  private final String httpMethod;
  private final String requestUri;
  private final int statusCode;
  private final EventSource explicitEventSource;

  /**
   * Constructs an {@code HttpRequestFailureEvent}.
   *
   * @param user the authenticated user, or {@code "unknown"} if authentication had not completed.
   * @param remoteAddress the client IP resolved by the filter (X-Forwarded-For or raw socket
   *     address). Stored explicitly because Iceberg and Lance servers do not install {@code
   *     RequestContextFilter}, so {@code RequestContext.getRemoteAddress()} may be unset.
   * @param httpMethod the HTTP method (e.g. {@code "GET"}, {@code "POST"}).
   * @param requestUri the request URI path (e.g. {@code "/api/metalakes/m1/catalogs"}).
   * @param statusCode the HTTP response status code (e.g. {@code 401}, {@code 404}).
   * @param eventSource identifies which server produced the event.
   */
  public HttpRequestFailureEvent(
      String user,
      String remoteAddress,
      String httpMethod,
      String requestUri,
      int statusCode,
      EventSource eventSource) {
    super(user, null, new HttpRequestException(httpMethod, requestUri, statusCode));
    this.explicitRemoteAddress = remoteAddress != null ? remoteAddress : "unknown";
    this.httpMethod = httpMethod;
    this.requestUri = requestUri;
    this.statusCode = statusCode;
    this.explicitEventSource = eventSource;
  }

  /** Returns {@link OperationType#UNKNOWN} — no operation was identified at the HTTP layer. */
  @Override
  public OperationType operationType() {
    return OperationType.UNKNOWN;
  }

  /**
   * Returns the explicitly-resolved client remote address. This overrides the base {@link
   * org.apache.gravitino.listener.api.event.Event} behaviour (which reads from {@link
   * org.apache.gravitino.utils.RequestContext}) so that events emitted by servers that do not
   * install {@code RequestContextFilter} still carry a correct address.
   */
  @Override
  public String remoteAddress() {
    return explicitRemoteAddress;
  }

  /** Returns the {@link EventSource} supplied at construction time. */
  @Override
  public EventSource eventSource() {
    return explicitEventSource;
  }

  /**
   * Returns HTTP-specific context that distinguishes this event from operation-layer events.
   *
   * <ul>
   *   <li>{@code http.method} — the HTTP verb
   *   <li>{@code http.uri} — the request URI path
   *   <li>{@code http.status} — the response status code as a string
   * </ul>
   */
  @Override
  public Map<String, String> customInfo() {
    return ImmutableMap.of(
        "http.method", httpMethod,
        "http.uri", requestUri,
        "http.status", String.valueOf(statusCode));
  }

  /** Returns the HTTP method. */
  public String httpMethod() {
    return httpMethod;
  }

  /** Returns the request URI. */
  public String requestUri() {
    return requestUri;
  }

  /** Returns the HTTP response status code. */
  public int statusCode() {
    return statusCode;
  }

  /**
   * A lightweight synthetic exception used as the {@link FailureEvent#exception()} carrier for
   * {@link HttpRequestFailureEvent}. It encodes the HTTP method, URI, and status so that log
   * formatters that render the exception message get readable output without needing to inspect
   * {@link HttpRequestFailureEvent#customInfo()} separately.
   */
  public static final class HttpRequestException extends RuntimeException {

    private HttpRequestException(String httpMethod, String requestUri, int statusCode) {
      super(
          String.format("HTTP request failed: %s %s -> %d", httpMethod, requestUri, statusCode),
          null,
          true,
          false /* suppress stack trace — this is a synthetic carrier, not a real exception */);
    }
  }
}
