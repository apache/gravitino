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
package org.apache.gravitino.server.authentication;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.web.HealthCheckPathMatcher;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.utils.PrincipalUtils;

public class AuthenticationFilter implements Filter {
  /**
   * The matcher used to identify health check paths that bypass authentication. Subclasses may
   * replace this with a server-specific matcher (e.g. {@code IcebergHealthCheckPathMatcher}).
   */
  protected HealthCheckPathMatcher healthCheckMatcher = new HealthCheckPathMatcher();

  private final List<Authenticator> filterAuthenticators;

  public AuthenticationFilter() {
    filterAuthenticators = null;
  }

  @VisibleForTesting
  AuthenticationFilter(List<Authenticator> authenticators) {
    this.filterAuthenticators = authenticators;
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    // Health check endpoints must be reachable without credentials so that Kubernetes
    // probes, load balancers, and global traffic managers can monitor server availability.
    // See org.apache.gravitino.server.web.rest.HealthOperations.
    if (isHealthCheckRequest(request)) {
      chain.doFilter(request, response);
      return;
    }
    try {
      List<Authenticator> authenticators;
      if (filterAuthenticators == null || filterAuthenticators.isEmpty()) {
        authenticators = ServerAuthenticator.getInstance().authenticators();
      } else {
        authenticators = filterAuthenticators;
      }
      HttpServletRequest req = (HttpServletRequest) request;
      Enumeration<String> headerData = req.getHeaders(AuthConstants.HTTP_HEADER_AUTHORIZATION);
      byte[] authData = null;
      if (headerData.hasMoreElements()) {
        authData = headerData.nextElement().getBytes(StandardCharsets.UTF_8);
      }

      // If token is supported by multiple authenticators, use the first by default.
      Principal principal = null;
      for (Authenticator authenticator : authenticators) {
        if (authenticator.supportsToken(authData) && authenticator.isDataFromToken()) {
          principal = authenticator.authenticateToken(authData);
          if (principal != null) {
            request.setAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, principal);
            break;
          }
        }
      }
      if (principal == null) {
        throw new UnauthorizedException("The provided credentials did not support");
      }
      PrincipalUtils.doAs(
          principal,
          () -> {
            chain.doFilter(request, response);
            return null;
          });
    } catch (UnauthorizedException ue) {
      HttpServletResponse resp = (HttpServletResponse) response;
      if (!ue.getChallenges().isEmpty()) {
        // For some authentication, HTTP response can provide some challenge information
        // to let client to create correct authenticated request.
        // Refer to https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/WWW-Authenticate
        for (String challenge : ue.getChallenges()) {
          if (!challenge.toLowerCase().startsWith("basic")) {
            resp.setHeader(AuthConstants.HTTP_CHALLENGE_HEADER, challenge);
          }
        }
      }
      sendAuthErrorResponse(resp, ue);
    } catch (Exception e) {
      HttpServletResponse resp = (HttpServletResponse) response;
      sendAuthErrorResponse(resp, e);
    }
  }

  /**
   * Sends a JSON error response when authentication fails. Subclasses can override this to
   * customize the error response format (e.g., Iceberg REST server returns Iceberg-specific JSON
   * error bodies).
   *
   * @param response the HTTP servlet response
   * @param exception the authentication exception
   */
  protected void sendAuthErrorResponse(HttpServletResponse response, Exception exception)
      throws IOException {
    int httpStatus;
    ErrorResponse errorResponse;

    if (exception instanceof UnauthorizedException) {
      httpStatus = HttpServletResponse.SC_UNAUTHORIZED;
      errorResponse =
          ErrorResponse.unauthorized(
              exception.getClass().getSimpleName(), exception.getMessage(), exception);
    } else if (exception instanceof ForbiddenException) {
      httpStatus = HttpServletResponse.SC_FORBIDDEN;
      errorResponse = ErrorResponse.forbidden(exception.getMessage(), exception);
    } else {
      httpStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
      errorResponse = ErrorResponse.internalError(exception.getMessage(), exception);
    }

    response.setStatus(httpStatus);
    response.setContentType("application/json");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    ObjectMapperProvider.objectMapper().writeValue(response.getWriter(), errorResponse);
  }

  /**
   * Returns {@code true} if the request targets a health check endpoint that should bypass
   * authentication, as determined by the configured {@link #healthCheckMatcher}.
   *
   * @param request the incoming servlet request
   * @return {@code true} if the request should skip authentication
   */
  protected boolean isHealthCheckRequest(ServletRequest request) {
    if (!(request instanceof HttpServletRequest)) {
      return false;
    }
    return healthCheckMatcher.isHealthCheckPath(((HttpServletRequest) request).getRequestURI());
  }

  @Override
  public void destroy() {}
}
