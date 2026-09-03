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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.audit.AuditLogRedactor;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.utils.RequestContext;

/**
 * A servlet filter that captures the client remote address and (raw, unredacted) query parameters
 * from each HTTP request and stores them in {@link RequestContext} so that audit event constructors
 * can read them on the same thread.
 *
 * <p>When a reverse proxy is in use, the real client IP is taken from the first entry of the {@code
 * X-Forwarded-For} header (a de-facto standard header set by reverse proxies; note that it is
 * trusted unconditionally — deployments where the server is reachable directly, without a trusted
 * reverse proxy, should be aware that clients can spoof this header). If the header is absent,
 * {@link HttpServletRequest#getRemoteAddr()} is used instead.
 *
 * <p>Query parameters are captured as-is, not redacted here. Redaction happens exactly once, at
 * audit-log format time, via {@link AuditLogRedactor}, applied uniformly to the fully-merged {@code
 * customInfo()} map regardless of which layer contributed which key — see {@link
 * AuditLogRedactor}'s class doc for why that single pass replaces redacting at every source
 * separately. Capturing and flattening the parameter map is skipped entirely when no {@link
 * EventBus} is supplied, since nothing will ever read the result.
 *
 * <p>The stored values are always cleared in a {@code finally} block to prevent thread-pool leaks.
 */
public class RequestContextFilter implements Filter {

  private static final String X_FORWARDED_FOR = "X-Forwarded-For";

  private final Optional<EventBus> eventBus;

  /** Constructs a {@code RequestContextFilter} that never captures query parameters. */
  public RequestContextFilter() {
    this(null);
  }

  /**
   * Constructs a {@code RequestContextFilter}.
   *
   * @param eventBus the event bus that will consume the captured query parameters; may be {@code
   *     null}, in which case query-parameter capture is skipped (remote-address capture still
   *     happens, since {@code HttpAuditFilter}'s own fallback events need it regardless).
   */
  public RequestContextFilter(@Nullable EventBus eventBus) {
    this.eventBus = Optional.ofNullable(eventBus);
  }

  @Override
  public void init(FilterConfig filterConfig) {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      if (request instanceof HttpServletRequest) {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        RequestContext.setRemoteAddress(resolveClientAddress(httpRequest));
        if (eventBus.isPresent()) {
          RequestContext.setRequestQueryParams(flattenParameterMap(httpRequest.getParameterMap()));
        }
      }
      chain.doFilter(request, response);
    } finally {
      RequestContext.clear();
    }
  }

  @Override
  public void destroy() {}

  private String resolveClientAddress(HttpServletRequest request) {
    String xForwardedFor = request.getHeader(X_FORWARDED_FOR);
    if (StringUtils.isNotBlank(xForwardedFor)) {
      return xForwardedFor.split(",")[0].trim();
    }
    return request.getRemoteAddr();
  }

  /**
   * Flattens a servlet-style parameter map to a single value per name (joining multi-valued
   * parameters with a comma), with no redaction — see the class doc for why that happens later.
   */
  private static Map<String, String> flattenParameterMap(Map<String, String[]> parameterMap) {
    if (parameterMap == null || parameterMap.isEmpty()) {
      return ImmutableMap.of();
    }
    ImmutableMap.Builder<String, String> flattened = ImmutableMap.builder();
    for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
      String[] values = entry.getValue();
      flattened.put(entry.getKey(), values == null ? "" : String.join(",", values));
    }
    return flattened.build();
  }
}
