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
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.FailureEvent;
import org.apache.gravitino.listener.api.event.OperationType;

/**
 * Represents an authorization denial that occurred inside {@code
 * MetadataAuthorizationMethodInterceptor}: the request was authenticated but the authorization
 * executor returned {@code false} for the requested operation, or user validation raised a {@link
 * ForbiddenException}.
 *
 * <p>This event carries rich context about the denial — the authenticated user, the resource
 * identifier, the Java method name, and the authorization expression — so auditors can reconstruct
 * exactly what was attempted and why it was refused.
 *
 * <p>Compared to a generic {@link HttpRequestFailureEvent} with {@code http.status=403}, this event
 * is distinguished by {@link OperationType#AUTHORIZATION_DENIAL} and by the non-null {@link
 * #identifier()} and {@link #customInfo()} fields. Dispatching this event before returning the 403
 * response also sets the {@code RequestContext.operationFailureFired} flag, preventing {@code
 * HttpAuditFilter} from emitting a duplicate HTTP-level event for the same request.
 */
@DeveloperApi
public final class AuthorizationDenialFailureEvent extends FailureEvent {

  private final String methodName;
  private final String expression;
  private final EventSource explicitEventSource;

  /**
   * Constructs an {@code AuthorizationDenialFailureEvent} attributed to the main Gravitino server.
   *
   * @param user the authenticated user whose request was denied.
   * @param accessMetadataName the {@link NameIdentifier} of the resource the user attempted to
   *     access. May be {@code null} when the denial occurs at the metalake-user validation level
   *     before a specific resource is resolved.
   * @param methodName the name of the intercepted Java method (e.g. {@code "loadTable"}).
   * @param expression the authorization expression that was evaluated (e.g. {@code "TABLE:LOAD"}).
   *     May be empty when the denial occurs before the expression is extracted.
   */
  public AuthorizationDenialFailureEvent(
      String user,
      @Nullable NameIdentifier accessMetadataName,
      String methodName,
      String expression) {
    this(user, accessMetadataName, methodName, expression, EventSource.GRAVITINO_SERVER);
  }

  /**
   * Constructs an {@code AuthorizationDenialFailureEvent} with an explicit {@link EventSource}. Use
   * this overload when the denial originates from a server other than the main Gravitino server
   * (e.g. Iceberg REST, Lance REST).
   *
   * @param user the authenticated user whose request was denied.
   * @param accessMetadataName the {@link NameIdentifier} of the resource the user attempted to
   *     access. May be {@code null}.
   * @param methodName the name of the intercepted Java method.
   * @param expression the authorization expression that was evaluated.
   * @param eventSource identifies which server produced this denial event.
   */
  public AuthorizationDenialFailureEvent(
      String user,
      @Nullable NameIdentifier accessMetadataName,
      String methodName,
      String expression,
      EventSource eventSource) {
    super(
        user,
        accessMetadataName,
        new ForbiddenException(
            "Authorization denied for user '%s' on operation '%s'", user, methodName));
    this.methodName = methodName;
    this.expression = expression != null ? expression : "";
    this.explicitEventSource = eventSource;
  }

  /** Returns {@link OperationType#AUTHORIZATION_DENIAL}. */
  @Override
  public OperationType operationType() {
    return OperationType.AUTHORIZATION_DENIAL;
  }

  /** Returns the {@link EventSource} supplied at construction time. */
  @Override
  public EventSource eventSource() {
    return explicitEventSource;
  }

  /**
   * Returns authorization-specific context that distinguishes this event from HTTP-level events.
   *
   * <ul>
   *   <li>{@code auth.method} — the intercepted Java method name
   *   <li>{@code auth.expression} — the authorization expression evaluated, or empty string if not
   *       yet resolved at the point of denial
   * </ul>
   *
   * <p>The denied resource name is available via {@link #identifier()} and is intentionally omitted
   * here to avoid duplication.
   */
  @Override
  public Map<String, String> customInfo() {
    return ImmutableMap.of(
        "auth.method", methodName,
        "auth.expression", expression);
  }

  /** Returns the intercepted Java method name. */
  public String methodName() {
    return methodName;
  }

  /** Returns the authorization expression that was evaluated. */
  public String expression() {
    return expression;
  }
}
