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
package org.apache.gravitino.lance.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.auth.IllegalActiveRolesException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.authentication.AuthenticationFilter;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.lance.namespace.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AuthenticationFilter} subclass for the Lance REST server that:
 *
 * <ul>
 *   <li>allows health check endpoints to bypass authentication via {@link
 *       LanceHealthCheckPathMatcher};
 *   <li>returns Lance-compatible JSON error responses on authentication failure instead of the
 *       default HTML error pages.
 * </ul>
 */
public class LanceAuthenticationFilter extends AuthenticationFilter {

  private static final Logger LOG = LoggerFactory.getLogger(LanceAuthenticationFilter.class);
  private static final ObjectMapper MAPPER = ObjectMapperProvider.objectMapper();

  public LanceAuthenticationFilter() {
    healthCheckMatcher = new LanceHealthCheckPathMatcher();
  }

  @Override
  protected void sendAuthErrorResponse(HttpServletResponse response, Exception exception)
      throws IOException {
    int status;
    String message;
    if (exception instanceof UnauthorizedException) {
      status = HttpServletResponse.SC_UNAUTHORIZED;
      message = exception.getMessage();
      if (message == null || message.isEmpty()) {
        message = "Authentication failed";
      }
    } else if (exception instanceof ForbiddenException) {
      status = HttpServletResponse.SC_FORBIDDEN;
      message = exception.getMessage();
      if (message == null || message.isEmpty()) {
        message = "Access denied";
      }
    } else if (exception instanceof IllegalActiveRolesException) {
      status = HttpServletResponse.SC_BAD_REQUEST;
      message = exception.getMessage();
      if (message == null || message.isEmpty()) {
        message = "Bad request";
      }
    } else {
      status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
      LOG.error("Authentication failure", exception);
      message = "Authentication failed";
    }

    ErrorResponse errorResponse = new ErrorResponse();
    errorResponse.setCode(status);
    errorResponse.setError(message);
    errorResponse.setDetail("");
    errorResponse.setInstance("");

    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    MAPPER.writeValue(response.getWriter(), errorResponse);
  }
}
