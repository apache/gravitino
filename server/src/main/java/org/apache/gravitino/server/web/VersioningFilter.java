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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(VersioningFilter.class);

  static final class MutableHttpServletRequest extends HttpServletRequestWrapper {

    private final Map<String, String> customHeaders;

    public MutableHttpServletRequest(HttpServletRequest request) {
      super(request);
      this.customHeaders = new HashMap<>();
    }

    public void putHeader(String name, String value) {
      this.customHeaders.put(name, value);
    }

    @Override
    public String getHeader(String name) {
      String headerValue = customHeaders.get(name);
      if (headerValue != null) {
        return headerValue;
      }
      return ((HttpServletRequest) getRequest()).getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaders(String name) {
      String headerValue = customHeaders.get(name);
      if (headerValue != null) {
        return Collections.enumeration(Collections.singletonList(headerValue));
      }
      return ((HttpServletRequest) getRequest()).getHeaders(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      List<String> combinedHeaderNames = new ArrayList<>(customHeaders.keySet());

      Enumeration<String> headerNames = ((HttpServletRequest) getRequest()).getHeaderNames();
      while (headerNames.hasMoreElements()) {
        combinedHeaderNames.add(headerNames.nextElement());
      }

      return Collections.enumeration(combinedHeaderNames);
    }
  }

  private static final Pattern VERSIONED_JSON_MEDIA_TYPE_REGEX =
      Pattern.compile("application/vnd\\.gravitino\\.v(\\d+)\\+json");
  private static final String ACCEPT_VERSION_HEADER = "Accept";
  private static final String CONTENT_TYPE_HEADER = "Content-Type";

  private static String getAcceptVersion(int version) {
    return String.format("application/vnd.gravitino.v%d+json", version);
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    Integer acceptVersion = versionFromHeaders(req.getHeaders(ACCEPT_VERSION_HEADER));
    if (acceptVersion != null) {
      if (isUnsupportedVersion(acceptVersion, response)) {
        return;
      }

      chain.doFilter(request, response);
      return;
    }

    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(req);
    Integer contentTypeVersion = versionFromHeader(req.getHeader(CONTENT_TYPE_HEADER));
    if (contentTypeVersion != null) {
      if (isUnsupportedVersion(contentTypeVersion, response)) {
        return;
      }

      mutableRequest.putHeader(ACCEPT_VERSION_HEADER, getAcceptVersion(contentTypeVersion));
    } else {
      ApiVersion defaultVersion = ApiVersion.defaultVersion();
      mutableRequest.putHeader(ACCEPT_VERSION_HEADER, getAcceptVersion(defaultVersion.version()));
    }

    chain.doFilter(mutableRequest, response);
  }

  private static Integer versionFromHeaders(Enumeration<String> headers) {
    while (headers.hasMoreElements()) {
      Integer version = versionFromHeader(headers.nextElement());
      if (version != null) {
        return version;
      }
    }

    return null;
  }

  private static Integer versionFromHeader(String value) {
    if (value == null) {
      return null;
    }

    Matcher matcher = VERSIONED_JSON_MEDIA_TYPE_REGEX.matcher(value);
    return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
  }

  private static boolean isUnsupportedVersion(int version, ServletResponse response)
      throws IOException {
    if (ApiVersion.isSupportedVersion(version)) {
      return false;
    }

    LOG.error("Unsupported version v{} in request header.", version);
    String message = String.format("Unsupported version v%d in request header", version);
    ErrorResponse errorResponse = ErrorResponse.illegalArguments(message);

    // Write the JSON ErrorResponse directly instead of calling HttpServletResponse#sendError, so
    // this filter -- which runs before Jersey ever sees the request -- doesn't fall through to
    // Jetty's default HTML error page.
    HttpServletResponse resp = (HttpServletResponse) response;
    resp.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
    resp.setContentType("application/json");
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    ObjectMapperProvider.objectMapper().writeValue(resp.getWriter(), errorResponse);
    return true;
  }

  @Override
  public void destroy() {}
}
