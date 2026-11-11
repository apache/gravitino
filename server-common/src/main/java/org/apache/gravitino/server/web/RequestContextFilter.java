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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
 * <p>The client address is resolved by a {@link ClientAddressResolver}. The {@code X-Forwarded-For}
 * header is honored only when the request arrives from a configured trusted proxy ({@code
 * trustedProxies} in {@link JettyServerConfig}); otherwise {@link
 * HttpServletRequest#getRemoteAddr()} is used, so a caller cannot choose the address that audit
 * events record by sending the header itself.
 *
 * <p>Query parameters are parsed from {@link HttpServletRequest#getQueryString()}, not from {@link
 * HttpServletRequest#getParameterMap()}. The latter also parses an {@code
 * application/x-www-form-urlencoded} request body when present, consuming the request input stream
 * in the process — a resource endpoint that later reads the body itself (a future OAuth2
 * token-style endpoint, for example) would see an empty body with no indication this filter is the
 * cause. Parsing the raw query string avoids that hazard entirely and matches what this class
 * actually claims to capture.
 *
 * <p>Query parameters are captured as-is, not redacted here. Redaction happens exactly once, at
 * audit-log format time, via {@link AuditLogRedactor}, applied uniformly to the fully-merged {@code
 * customInfo()} map regardless of which layer contributed which key — see {@link
 * AuditLogRedactor}'s class doc for why that single pass replaces redacting at every source
 * separately. Capturing the query string is skipped entirely when no {@link EventBus} is supplied,
 * since nothing will ever read the result.
 *
 * <p>The number of distinct parameter names captured is capped at {@value #MAX_PARAMETERS}, and any
 * single value is truncated to {@value #MAX_VALUE_LENGTH} characters (marked with {@value
 * #TRUNCATED_SUFFIX}) — a caller cannot grow every event constructed during a request, and every
 * audit line written for it, by attaching an arbitrarily large query string.
 *
 * <p>The stored values are always cleared in a {@code finally} block to prevent thread-pool leaks.
 */
public class RequestContextFilter implements Filter {

  /** Maximum number of distinct query-parameter names captured per request. */
  static final int MAX_PARAMETERS = 50;

  /** Maximum length of a single captured (post-join) parameter value, before truncation. */
  static final int MAX_VALUE_LENGTH = 256;

  /** Appended to a value that was cut short at {@link #MAX_VALUE_LENGTH}. */
  static final String TRUNCATED_SUFFIX = "...(truncated)";

  private final Optional<EventBus> eventBus;
  private final ClientAddressResolver clientAddressResolver;

  /**
   * Constructs a {@code RequestContextFilter} that never captures query parameters and trusts no
   * proxy.
   */
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
    this(eventBus, ClientAddressResolver.noTrustedProxies());
  }

  /**
   * Constructs a {@code RequestContextFilter}.
   *
   * @param eventBus the event bus that will consume the captured query parameters; may be {@code
   *     null}, in which case query-parameter capture is skipped.
   * @param clientAddressResolver resolves the client address from the request, honoring {@code
   *     X-Forwarded-For} only from trusted proxies.
   */
  public RequestContextFilter(
      @Nullable EventBus eventBus, ClientAddressResolver clientAddressResolver) {
    this.eventBus = Optional.ofNullable(eventBus);
    this.clientAddressResolver = clientAddressResolver;
  }

  @Override
  public void init(FilterConfig filterConfig) {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      if (request instanceof HttpServletRequest) {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        RequestContext.setRemoteAddress(clientAddressResolver.resolve(httpRequest));
        if (eventBus.isPresent()) {
          RequestContext.setRequestQueryParams(parseQueryString(httpRequest.getQueryString()));
        }
      }
      chain.doFilter(request, response);
    } finally {
      RequestContext.clear();
    }
  }

  @Override
  public void destroy() {}

  /**
   * Parses a raw HTTP query string into a name-to-value map (joining multi-valued parameters with a
   * comma), with no redaction — see the class doc for why that happens later. Bounded by {@link
   * #MAX_PARAMETERS} and {@link #MAX_VALUE_LENGTH} so a caller cannot inflate every event
   * constructed during this request with an arbitrarily large query string.
   *
   * @param queryString the raw (still percent-encoded) query string, or {@code null}
   * @return an immutable, bounded, decoded parameter map; empty if {@code queryString} is {@code
   *     null} or blank
   */
  private static Map<String, String> parseQueryString(String queryString) {
    if (StringUtils.isBlank(queryString)) {
      return ImmutableMap.of();
    }
    Map<String, List<String>> multiValued = new LinkedHashMap<>();
    for (String pair : queryString.split("&")) {
      if (pair.isEmpty()) {
        continue;
      }
      int equalsIndex = pair.indexOf('=');
      String rawName = equalsIndex >= 0 ? pair.substring(0, equalsIndex) : pair;
      String rawValue = equalsIndex >= 0 ? pair.substring(equalsIndex + 1) : "";
      String name = decode(rawName);
      // Once the cap is hit, only accumulate more values for names already being tracked — never
      // start tracking a new name.
      if (!multiValued.containsKey(name) && multiValued.size() >= MAX_PARAMETERS) {
        continue;
      }
      multiValued.computeIfAbsent(name, k -> new ArrayList<>()).add(decode(rawValue));
    }
    ImmutableMap.Builder<String, String> flattened = ImmutableMap.builder();
    multiValued.forEach((name, values) -> flattened.put(name, truncate(String.join(",", values))));
    return flattened.build();
  }

  private static String truncate(String value) {
    return value.length() > MAX_VALUE_LENGTH
        ? value.substring(0, MAX_VALUE_LENGTH) + TRUNCATED_SUFFIX
        : value;
  }

  /**
   * Decodes an {@code application/x-www-form-urlencoded} query-string component (the encoding every
   * servlet container assumes for a query string). Malformed percent-encoding — always possible,
   * since this is attacker-influenced input — falls back to the raw component instead of throwing,
   * so one bad parameter cannot fail the whole request just to be audited.
   */
  private static String decode(String value) {
    try {
      return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      // UTF-8 is always supported by the JVM; unreachable in practice.
      return value;
    } catch (IllegalArgumentException e) {
      return value;
    }
  }
}
