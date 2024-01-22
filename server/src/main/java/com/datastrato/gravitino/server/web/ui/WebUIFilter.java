/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.ui;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

// This filter is used to serve static HTML files from the Gravitino WEB UI.
// https://nextjs.org/docs/pages/building-your-application/deploying/static-exports#deploying
public class WebUIFilter implements Filter {
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String path = httpRequest.getRequestURI();
    String lastPathSegment = path.substring(path.lastIndexOf("/") + 1);
    if (path.equals("/")) {
      // Redirect to the index page.
      httpRequest.getRequestDispatcher("/ui/index.html").forward(request, response);
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
