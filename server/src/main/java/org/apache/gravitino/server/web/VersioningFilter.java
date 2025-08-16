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
    public Enumeration<String> getHeaderNames() {
      List<String> combinedHeaderNames = new ArrayList<>(customHeaders.keySet());

      Enumeration<String> headerNames = ((HttpServletRequest) getRequest()).getHeaderNames();
      while (headerNames.hasMoreElements()) {
        combinedHeaderNames.add(headerNames.nextElement());
      }

      return Collections.enumeration(combinedHeaderNames);
    }
  }

  private static final Pattern ACCEPT_VERSION_REGEX =
      Pattern.compile("application/vnd\\.gravitino\\.v(\\d+)\\+json");
  private static final String ACCEPT_VERSION_HEADER = "Accept";

  private static String getAcceptVersion(int version) {
    return String.format("application/vnd.gravitino.v%d+json", version);
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    Enumeration<String> acceptHeader = req.getHeaders(ACCEPT_VERSION_HEADER);
    while (acceptHeader.hasMoreElements()) {
      String value = acceptHeader.nextElement();

      // If version accept header is set, then we need to check if it is supported.
      Matcher m = ACCEPT_VERSION_REGEX.matcher(value);
      if (m.find()) {
        int version = Integer.parseInt(m.group(1));

        if (!ApiVersion.isSupportedVersion(version)) {
          LOG.error("Unsupported version v{} in Request Header {}.", version, value);

          HttpServletResponse resp = (HttpServletResponse) response;
          resp.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
        } else {
          chain.doFilter(request, response);
        }

        return;
      }
    }

    // If no version accept header not is set, then we need to set the latest version.
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(req);
    ApiVersion latest = ApiVersion.latestVersion();
    mutableRequest.putHeader(ACCEPT_VERSION_HEADER, getAcceptVersion(latest.version()));

    chain.doFilter(mutableRequest, response);
  }

  @Override
  public void destroy() {}
}
