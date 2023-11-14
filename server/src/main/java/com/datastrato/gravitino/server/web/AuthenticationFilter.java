/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.auth.Authenticator;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.utils.Constants;
import java.io.IOException;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthenticationFilter implements Filter {

  private final Authenticator authenticator;

  public AuthenticationFilter(Authenticator authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      HttpServletRequest req = (HttpServletRequest) request;
      Enumeration<String> headerData = req.getHeaders(Constants.HTTP_HEADER_NAME);
      String authData = null;
      if (headerData.hasMoreElements()) {
        authData = headerData.nextElement();
      }
      if (authenticator.isDataFromHTTP()) {
        authenticator.authenticateHTTPHeader(authData);
      }
      chain.doFilter(request, response);
    } catch (UnauthorizedException ue) {
      HttpServletResponse resp = (HttpServletResponse) response;
      resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, ue.getMessage());
    }
  }

  @Override
  public void destroy() {}
}
