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
package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.server.authentication.AuthenticationFilter;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.http.HttpStatus;

/**
 * An {@link AuthenticationFilter} subclass for the Iceberg REST server that produces JSON error
 * responses conforming to the Iceberg REST API specification.
 *
 * <p>When authentication fails, the default {@link AuthenticationFilter} calls {@code
 * resp.sendError()} which produces HTML error pages via Jetty's default error handler. This
 * subclass overrides the error response to write a proper Iceberg {@link ErrorResponse} JSON body,
 * which Iceberg REST clients (e.g., the Java {@code RESTCatalog}) expect.
 */
public class IcebergAuthenticationFilter extends AuthenticationFilter {

  private static final ObjectMapper MAPPER = IcebergObjectMapper.getInstance();

  // Error type names matching the Iceberg REST API specification examples.
  // See https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
  static final Map<Integer, String> ERROR_TYPE_NAMES =
      ImmutableMap.<Integer, String>builder()
          .put(HttpServletResponse.SC_BAD_REQUEST, "BadRequestException")
          .put(HttpServletResponse.SC_UNAUTHORIZED, "NotAuthorizedException")
          .put(HttpServletResponse.SC_FORBIDDEN, "NotAuthorizedException")
          .put(HttpServletResponse.SC_NOT_FOUND, "NoSuchResourceException")
          .put(HttpServletResponse.SC_CONFLICT, "AlreadyExistsException")
          .put(HttpServletResponse.SC_NOT_ACCEPTABLE, "UnsupportedOperationException")
          .put(422, "UnprocessableEntityException")
          .put(419, "AuthenticationTimeoutException")
          .put(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "SlowDownException")
          .put(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "InternalServerError")
          .build();

  @Override
  protected void sendAuthErrorResponse(HttpServletResponse response, int status, String message)
      throws IOException {
    if (message == null || message.isEmpty()) {
      message = HttpStatus.getMessage(status);
    }

    String type = ERROR_TYPE_NAMES.getOrDefault(status, HttpStatus.getMessage(status));
    ErrorResponse errorResponse = IcebergRESTUtils.errorResponse(status, type, message);

    response.setStatus(status);
    response.setContentType("application/json");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    MAPPER.writeValue(response.getWriter(), errorResponse);
  }
}
