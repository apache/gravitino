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
package org.apache.gravitino.paimon.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.ActiveRolesParser;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.IllegalActiveRolesException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.authentication.AuthenticationFilter;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AuthenticationFilter} subclass for the Paimon REST server.
 *
 * <p>Two modes:
 *
 * <ul>
 *   <li><b>Shared bearer token</b> (when the {@code token} service configuration is set): the
 *       filter itself validates that the {@code Authorization: Bearer <token>} header carries the
 *       configured shared token. This works in every deployment mode because it does not depend on
 *       the Gravitino authenticator framework.
 *   <li><b>Gravitino authenticator framework</b> (default): validation is delegated to the
 *       configured Gravitino authenticators, exactly like the main Gravitino server.
 * </ul>
 *
 * <p>Errors are rendered using the Paimon wire format so that clients can decode them.
 */
public class PaimonAuthenticationFilter extends AuthenticationFilter {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonAuthenticationFilter.class);

  private static final String AUTHORIZATION_HEADER = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";

  /** Shared token mapped to a fixed principal when only {@code token} is configured. */
  static final String SHARED_PRINCIPAL = "shared";

  private final Optional<String> expectedBearerToken;
  private final Map<String, String> tokensByPrincipal;

  /** Creates a filter that delegates to the Gravitino authenticators. */
  public PaimonAuthenticationFilter() {
    this(null, Collections.emptyMap());
  }

  /**
   * Creates the filter with an optional shared bearer token.
   *
   * @param expectedBearerToken the shared token clients must present, null or empty to delegate
   *     validation to the Gravitino authenticator framework
   */
  public PaimonAuthenticationFilter(String expectedBearerToken) {
    this(expectedBearerToken, Collections.emptyMap());
  }

  /**
   * Creates the filter with per-principal bearer tokens (and optionally a shared token).
   *
   * @param sharedToken the shared token mapped to the fixed {@code shared} principal, may be null
   * @param principalTokens mapping of principal name to the token clients must present
   */
  public PaimonAuthenticationFilter(String sharedToken, Map<String, String> principalTokens) {
    super();
    healthCheckMatcher = new PaimonHealthCheckPathMatcher();
    this.expectedBearerToken =
        StringUtils.isBlank(sharedToken) ? Optional.empty() : Optional.of(sharedToken.trim());
    this.tokensByPrincipal = new HashMap<>();
    principalTokens.forEach(
        (principal, token) -> {
          if (StringUtils.isNotBlank(token)) {
            this.tokensByPrincipal.put(token.trim(), principal);
          }
        });
  }

  /**
   * Validates the shared bearer token when configured; otherwise delegates to the Gravitino
   * authenticator framework.
   *
   * @param request the servlet request
   * @param response the servlet response
   * @param chain the filter chain
   * @throws IOException on IO failures
   * @throws ServletException on servlet failures
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (expectedBearerToken.isPresent() || !tokensByPrincipal.isEmpty()) {
      if (isHealthCheckRequest(request)) {
        // Health probes must be reachable without credentials.
        chain.doFilter(request, response);
        return;
      }
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      String authorization = httpRequest.getHeader(AUTHORIZATION_HEADER);
      if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
        sendAuthErrorResponse(httpResponse, new NotAuthorizedException("Missing bearer token"));
        return;
      }
      String token = authorization.substring(BEARER_PREFIX.length()).trim();
      if (!matches(expectedBearerToken, token)) {
        sendAuthErrorResponse(httpResponse, new NotAuthorizedException("Invalid bearer token"));
        return;
      }
      String principal =
          tokensByPrincipal.getOrDefault(
              token, expectedBearerToken.isPresent() ? SHARED_PRINCIPAL : token);
      try {
        UserPrincipal userPrincipal =
            new UserPrincipal(principal)
                .withActiveRoles(
                    ActiveRolesParser.parse(
                        httpRequest.getHeader(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER)));
        httpRequest.setAttribute(
            AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME, userPrincipal);
        PrincipalUtils.doAs(
            userPrincipal,
            () -> {
              chain.doFilter(request, response);
              return null;
            });
      } catch (IOException | ServletException e) {
        // Failures of the downstream processing are not authentication problems.
        throw e;
      } catch (UnauthorizedException | IllegalActiveRolesException e) {
        sendAuthErrorResponse(httpResponse, e);
      } catch (Exception e) {
        // Downstream errors are not authentication problems and their messages may contain
        // internal details: log fully, return a sanitized body.
        LOG.error("Error processing authenticated request", e);
        if (!httpResponse.isCommitted()) {
          httpResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          httpResponse.setContentType("application/json");
          httpResponse.setCharacterEncoding(StandardCharsets.UTF_8.name());
          RESTApi.OBJECT_MAPPER.writeValue(
              httpResponse.getWriter(),
              new ErrorResponse(null, null, "Internal server error", 500));
        }
      }
      return;
    }
    super.doFilter(request, response, chain);
  }

  private boolean matches(Optional<String> expected, String token) {
    if (expected.isPresent() && expected.get().equals(token)) {
      return true;
    }
    return tokensByPrincipal.containsKey(token);
  }

  /**
   * Writes the Paimon wire response for a request rejected during authentication.
   *
   * @param response the servlet response
   * @param exception the failure encountered while handling the request
   * @throws IOException if the response cannot be written
   */
  @Override
  protected void sendAuthErrorResponse(HttpServletResponse response, Exception exception)
      throws IOException {
    if (response.isCommitted()) {
      return;
    }
    int status;
    String message = exception.getMessage();
    if (exception instanceof UnauthorizedException || exception instanceof NotAuthorizedException) {
      status = HttpServletResponse.SC_UNAUTHORIZED;
      message = message == null ? "Not authorized" : "Not authorized: " + message;
    } else if (exception instanceof IllegalActiveRolesException) {
      status = HttpServletResponse.SC_BAD_REQUEST;
    } else if (exception instanceof ForbiddenException) {
      status = HttpServletResponse.SC_FORBIDDEN;
    } else {
      status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
      message = "Internal server error";
    }
    ErrorResponse errorResponse = new ErrorResponse(null, null, message, status);
    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    RESTApi.OBJECT_MAPPER.writeValue(response.getWriter(), errorResponse);
  }
}
