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
package org.apache.gravitino.server.web.ui;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

// This filter is used to serve static HTML files from the Apache Gravitino WEB UI.
// https://nextjs.org/docs/pages/building-your-application/deploying/static-exports#deploying
public class WebUIFilter implements Filter {
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String path = httpRequest.getRequestURI();
    String lastPathSegment = path.substring(path.lastIndexOf("/") + 1);

    if (path.equals("/") || path.equals("/ui") || path.equals("/ui/")) {
      // Redirect to the index page.
      httpRequest.getRequestDispatcher("/ui/index.html").forward(request, response);

    } else if (path.startsWith("/ui/") && path.endsWith("/")) {
      httpRequest.getRequestDispatcher(path + "index.html").forward(request, response);

    } else if (path.startsWith("/ui/") && !lastPathSegment.contains(".")) {
      // Redirect to the static HTML file.
      httpRequest.getRequestDispatcher(path + ".html").forward(request, response);

    } else {
      // Continue processing the rest of the filter chain.
      chain.doFilter(request, response);
    }
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void destroy() {}
}
