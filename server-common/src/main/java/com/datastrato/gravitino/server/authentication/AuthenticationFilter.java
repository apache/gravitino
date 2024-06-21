/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Set;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AuthenticationFilter implements Filter {

  private final Authenticator filterAuthenticator;

  private static final Set<String> AUTHENTICATION_EXCLUDE_PATH = ImmutableSet.of("/api/version");

  public AuthenticationFilter() {
    filterAuthenticator = null;
  }

  @VisibleForTesting
  AuthenticationFilter(Authenticator authenticator) {
    this.filterAuthenticator = authenticator;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    String path = httpRequest.getRequestURI();
    if (AUTHENTICATION_EXCLUDE_PATH.contains(path)) {
      chain.doFilter(request, response);
      return;
    }

    try {
      Authenticator authenticator;
      if (filterAuthenticator == null) {
        authenticator = ServerAuthenticator.getInstance().authenticator();
      } else {
        authenticator = filterAuthenticator;
      }
      HttpServletRequest req = (HttpServletRequest) request;
      Enumeration<String> headerData = req.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION);
      byte[] authData = null;
      if (headerData.hasMoreElements()) {
        authData = headerData.nextElement().getBytes(StandardCharsets.UTF_8);
      }
      if (authenticator.isDataFromToken()) {
        Principal principal = authenticator.authenticateToken(authData);
        request.setAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, principal);
      }
      chain.doFilter(request, response);
    } catch (UnauthorizedException ue) {
      HttpServletResponse resp = (HttpServletResponse) response;
      if (!ue.getChallenges().isEmpty()) {
        // For some authentication, HTTP response can provide some challenge information
        // to let client to create correct authenticated request.
        // Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/WWW-Authenticate
        for (String challenge : ue.getChallenges()) {
          resp.setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, challenge);
        }
      }
      resp.sendError(HttpServletResponse.SC_UNAUTHORIZED, ue.getMessage());
    }
  }

  @Override
  public void destroy() {}
}
