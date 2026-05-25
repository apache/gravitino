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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.utils.RequestContext;

/**
 * A servlet filter that captures the client remote address from each HTTP request and stores it in
 * {@link RequestContext} so that audit event constructors can read it on the same thread.
 *
 * <p>When a reverse proxy is in use, the real client IP is taken from the first entry of the {@code
 * X-Forwarded-For} header (a de-facto standard header set by reverse proxies; note that it is
 * trusted unconditionally — deployments where the server is reachable directly, without a trusted
 * reverse proxy, should be aware that clients can spoof this header). If the header is absent,
 * {@link HttpServletRequest#getRemoteAddr()} is used instead.
 *
 * <p>The stored value is always cleared in a {@code finally} block to prevent thread-pool leaks.
 */
public class RequestContextFilter implements Filter {

  private static final String X_FORWARDED_FOR = "X-Forwarded-For";

  @Override
  public void init(FilterConfig filterConfig) {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      if (request instanceof HttpServletRequest) {
        RequestContext.setRemoteAddress(resolveClientAddress((HttpServletRequest) request));
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
}
