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
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.OperationStatus;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * A fallback audit event for HTTP requests that complete with a 2xx/3xx status but for which no
 * operation-layer success {@link Event} was dispatched — for example, a REST endpoint that has not
 * (yet) been wired into the operation dispatcher/event system. It is emitted by {@code
 * HttpAuditFilter} so that such endpoints still produce an audit record (method, URI, status, query
 * parameters) instead of none at all.
 *
 * <p>Unlike operation-layer success events, this event carries no {@link
 * org.apache.gravitino.NameIdentifier} (the resource was never resolved as a structured entity) and
 * its {@link #operationType()} is always {@link OperationType#UNKNOWN}. Once an endpoint gains its
 * own structured operation-layer event, that event suppresses this fallback for the same request —
 * see {@code RequestContext#markOperationSuccessFired()}.
 *
 * <p>This is the success-path counterpart of {@link HttpRequestFailureEvent}.
 */
@DeveloperApi
public final class HttpRequestEvent extends Event {

  private final String explicitRemoteAddress;
  private final String httpMethod;
  private final String requestUri;
  private final int statusCode;
  private final EventSource explicitEventSource;

  /**
   * Constructs an {@code HttpRequestEvent}.
   *
   * @param user the authenticated user, or {@code "unknown"} if authentication had not completed.
   * @param remoteAddress the client IP resolved by the filter (X-Forwarded-For or raw socket
   *     address). Stored explicitly, rather than relying on the base {@link Event} behaviour of
   *     reading {@code RequestContext.getRemoteAddress()} at construction time, so this event still
   *     carries a correct address even on a server that does not install {@code
   *     RequestContextFilter} at all (all servers in this codebase currently do, but this keeps the
   *     event resilient to one that doesn't).
   * @param httpMethod the HTTP method (e.g. {@code "GET"}, {@code "POST"}).
   * @param requestUri the request URI path (e.g. {@code "/search/query"}).
   * @param statusCode the HTTP response status code (e.g. {@code 200}).
   * @param eventSource identifies which server produced the event.
   */
  public HttpRequestEvent(
      String user,
      String remoteAddress,
      String httpMethod,
      String requestUri,
      int statusCode,
      EventSource eventSource) {
    super(user, null);
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

  /** {@inheritDoc} */
  @Override
  public OperationStatus operationStatus() {
    return OperationStatus.SUCCESS;
  }

  /**
   * Returns the explicitly-resolved client remote address supplied at construction time, rather
   * than the base {@link Event} behaviour of reading it from {@link
   * org.apache.gravitino.utils.RequestContext}. See the constructor's {@code remoteAddress}
   * parameter doc for why.
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
   * Returns HTTP-specific context that distinguishes this event from operation-layer events. Merged
   * automatically by {@link Event#customInfo()} with the request's automatically captured query
   * parameters; these keys always win on a collision with a query parameter of the same name.
   *
   * <ul>
   *   <li>{@code http.method} — the HTTP verb
   *   <li>{@code http.uri} — the request URI path
   *   <li>{@code http.status} — the response status code as a string
   * </ul>
   */
  @Override
  protected Map<String, String> ownCustomInfo() {
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
}
