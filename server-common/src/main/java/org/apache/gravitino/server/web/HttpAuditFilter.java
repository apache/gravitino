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

import java.io.IOException;
import java.security.Principal;
import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.server.HttpRequestFailureEvent;
import org.apache.gravitino.utils.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A servlet filter that emits an {@link HttpRequestFailureEvent} for every HTTP request that
 * completes with a 4xx or 5xx status code and for which no operation-layer failure event was
 * already dispatched on the same request thread.
 *
 * <p><strong>Filter chain position:</strong> this filter must be registered <em>after</em> {@link
 * RequestContextFilter} (so the remote address is already populated) but <em>before</em> {@link
 * org.apache.gravitino.server.authentication.AuthenticationFilter} (so it observes 401
 * authentication failures as well as downstream 403 authorization denials). It is safe to install
 * on servers that do not use {@code RequestContextFilter} (Iceberg REST, Lance REST) because it
 * resolves the client address independently.
 *
 * <p><strong>Double-logging prevention:</strong> when an operation-layer failure event (e.g. {@code
 * LoadTableFailureEvent}, {@code AuthorizationDenialFailureEvent}) has already been dispatched via
 * {@link EventBus}, {@link RequestContext#markOperationFailureFired()} is set on the request
 * thread. This filter checks that flag in the {@code finally} block and skips emitting its own
 * {@link HttpRequestFailureEvent} if the flag is set.
 *
 * <p><strong>Success + 5xx edge case:</strong> if an operation dispatcher emits a {@code
 * SuccessEvent} (flag not set) but the HTTP layer subsequently fails with a 5xx (e.g. JSON
 * serialization error), this filter will emit an {@link HttpRequestFailureEvent} in addition to the
 * success event already in the audit log. Both entries are correct — the operation itself succeeded
 * but the response delivery failed — and are intentionally preserved.
 *
 * <p><strong>Health check exclusion:</strong> requests targeting {@code /health}, {@code
 * /health/*}, {@code /health.html}, {@code /api/health}, and {@code /api/health/*} are silently
 * passed through without audit logging to avoid polluting the audit log with probe traffic.
 *
 * <p><strong>Exception escape:</strong> if an uncaught {@link Throwable} escapes from the filter
 * chain and the captured status is still 200 (i.e. no downstream component set an error code), the
 * captured status is promoted to 500 before the {@code finally} block runs, ensuring that the audit
 * event reflects the actual error condition.
 */
public class HttpAuditFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(HttpAuditFilter.class);
  private static final String X_FORWARDED_FOR = "X-Forwarded-For";

  @Nullable private final EventBus eventBus;
  private final EventSource eventSource;

  /**
   * Constructs an {@code HttpAuditFilter}.
   *
   * @param eventBus the event bus used to dispatch {@link HttpRequestFailureEvent}s; may be {@code
   *     null}, in which case the filter is a pass-through no-op (useful when no audit listener is
   *     configured).
   * @param eventSource identifies which server this filter instance is installed on; included in
   *     every emitted {@link HttpRequestFailureEvent}.
   */
  public HttpAuditFilter(@Nullable EventBus eventBus, EventSource eventSource) {
    this.eventBus = eventBus;
    this.eventSource = eventSource;
  }

  @Override
  public void init(FilterConfig filterConfig) {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // Pass through when audit is not configured or request is not HTTP.
    if (eventBus == null || !(request instanceof HttpServletRequest)) {
      chain.doFilter(request, response);
      return;
    }

    HttpServletRequest httpRequest = (HttpServletRequest) request;
    // Defensive cleanup at request entry in case a pooled thread leaked stale state.
    RequestContext.resetOperationFailureFired();
    if (isHealthCheckRequest(httpRequest)) {
      chain.doFilter(request, response);
      return;
    }

    StatusCapturingResponseWrapper wrappedResponse =
        new StatusCapturingResponseWrapper((HttpServletResponse) response);
    Throwable chainException = null;
    try {
      chain.doFilter(httpRequest, wrappedResponse);
    } catch (Throwable t) {
      chainException = t;
      // Promote to 500 so the finally block emits an event for this escaped exception.
      if (wrappedResponse.getCapturedStatus() < 400) {
        wrappedResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } finally {
      try {
        if (!RequestContext.isOperationFailureFired()) {
          int status = wrappedResponse.getCapturedStatus();
          if (status >= 400) {
            String user = resolveUser(httpRequest);
            String remoteAddress = resolveClientAddress(httpRequest);
            HttpRequestFailureEvent event =
                new HttpRequestFailureEvent(
                    user,
                    remoteAddress,
                    httpRequest.getMethod(),
                    httpRequest.getRequestURI(),
                    status,
                    eventSource);
            eventBus.dispatchEvent(event);
          }
        }
      } catch (Exception e) {
        LOG.error(
            "Failed to dispatch HTTP audit event for {} {}",
            httpRequest.getMethod(),
            httpRequest.getRequestURI(),
            e);
      } finally {
        // Always clear the flag to prevent ThreadLocal leaks across pooled threads.
        RequestContext.resetOperationFailureFired();
      }
    }

    // Re-throw any exception that escaped the chain after cleanup is complete.
    if (chainException instanceof Error) {
      throw (Error) chainException;
    } else if (chainException instanceof RuntimeException) {
      throw (RuntimeException) chainException;
    } else if (chainException instanceof IOException) {
      throw (IOException) chainException;
    } else if (chainException instanceof ServletException) {
      throw (ServletException) chainException;
    } else if (chainException != null) {
      throw new ServletException(chainException);
    }
  }

  @Override
  public void destroy() {}

  private String resolveUser(HttpServletRequest request) {
    Object principalObj =
        request.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    if (principalObj instanceof Principal) {
      return ((Principal) principalObj).getName();
    }
    // The attribute is only set when authentication succeeds (AuthenticationFilter calls
    // request.setAttribute before chain.doFilter). Absent attribute means auth was rejected
    // before any principal was established. PrincipalUtils.getCurrentUserName() must not be used
    // here — it returns "anonymous" when no Subject.doAs is active, making a rejected request
    // indistinguishable from a legitimately anonymous one.
    return "unknown";
  }

  private String resolveClientAddress(HttpServletRequest request) {
    String xForwardedFor = request.getHeader(X_FORWARDED_FOR);
    if (StringUtils.isNotBlank(xForwardedFor)) {
      return xForwardedFor.split(",")[0].trim();
    }
    return request.getRemoteAddr();
  }

  private boolean isHealthCheckRequest(HttpServletRequest request) {
    String path = request.getRequestURI();
    if (path == null) {
      return false;
    }
    return path.equals("/health")
        || path.startsWith("/health/")
        || path.equals("/health.html")
        || path.equals("/api/health")
        || path.startsWith("/api/health/");
  }

  /**
   * A {@link HttpServletResponseWrapper} that intercepts all status-code-setting methods so that
   * the final HTTP status is available after {@link FilterChain#doFilter} returns.
   *
   * <p>All five status-mutating entry points are overridden: {@link #setStatus(int)}, {@link
   * #setStatus(int, String)}, {@link #sendError(int)}, {@link #sendError(int, String)}, and {@link
   * #reset()} (which resets the captured status back to 200).
   */
  static final class StatusCapturingResponseWrapper extends HttpServletResponseWrapper {

    private int capturedStatus = HttpServletResponse.SC_OK;

    /**
     * Constructs a wrapper around the given response.
     *
     * @param response the underlying HTTP response to delegate to
     */
    StatusCapturingResponseWrapper(HttpServletResponse response) {
      super(response);
    }

    /**
     * Returns the last HTTP status code recorded on this response. Defaults to {@code 200} if no
     * status-mutating method has been called.
     *
     * @return the captured HTTP status code
     */
    int getCapturedStatus() {
      return capturedStatus;
    }

    @Override
    public void setStatus(int sc) {
      capturedStatus = sc;
      super.setStatus(sc);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void setStatus(int sc, String sm) {
      capturedStatus = sc;
      super.setStatus(sc, sm);
    }

    @Override
    public void sendError(int sc) throws IOException {
      capturedStatus = sc;
      super.sendError(sc);
    }

    @Override
    public void sendError(int sc, String msg) throws IOException {
      capturedStatus = sc;
      super.sendError(sc, msg);
    }

    /**
     * Resets the captured status to {@code 200} in addition to delegating to the wrapped response.
     */
    @Override
    public void reset() {
      capturedStatus = HttpServletResponse.SC_OK;
      super.reset();
    }
  }
}
