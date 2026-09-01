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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ErrorHandler;

/**
 * A Jetty {@link ErrorHandler} that writes the same structured {@link ErrorResponse} JSON body used
 * by the rest of the API, instead of Jetty's default HTML error page, for requests under a given
 * path prefix.
 *
 * <p>A typed {@code @PathParam} (e.g. an {@code int} model version) is matched and converted by
 * Jersey before any resource method or registered {@code javax.ws.rs.ext.ExceptionMapper} runs.
 * When that conversion fails, Jersey's {@code ServletContainer} calls {@code
 * HttpServletResponse#sendError} directly, which Jetty would otherwise render with this server's
 * default, HTML-producing {@link ErrorHandler}. Installing this handler ensures such routing-level
 * failures still return a Gravitino {@link ErrorResponse}, consistent with every other error on the
 * API.
 */
public class JsonErrorHandler extends ErrorHandler {

  private final String apiPathPrefix;

  /**
   * Creates a new {@link JsonErrorHandler}.
   *
   * @param apiPathPrefix requests whose URI starts with this prefix get a JSON {@link
   *     ErrorResponse} body; all other requests fall back to Jetty's default HTML error handling.
   */
  public JsonErrorHandler(String apiPathPrefix) {
    this.apiPathPrefix = apiPathPrefix;
  }

  @Override
  public void handle(
      String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    if (!isApiPath(request)) {
      super.handle(target, baseRequest, request, response);
      return;
    }

    int status = response.getStatus();
    String message = (String) request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
    if (StringUtils.isBlank(message)) {
      message = HttpStatus.getMessage(status);
    }

    ErrorResponse errorResponse = toErrorResponse(status, message);
    response.setContentType(MediaType.APPLICATION_JSON);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    ObjectMapper mapper = ObjectMapperProvider.objectMapper();
    try (PrintWriter writer = response.getWriter()) {
      mapper.writeValue(writer, errorResponse);
    }
    baseRequest.setHandled(true);
  }

  boolean isApiPath(HttpServletRequest request) {
    return StringUtils.startsWith(request.getRequestURI(), apiPathPrefix);
  }

  private static ErrorResponse toErrorResponse(int status, String message) {
    switch (status) {
      case HttpServletResponse.SC_BAD_REQUEST:
        return ErrorResponse.illegalArguments(message);
      case HttpServletResponse.SC_UNAUTHORIZED:
        return ErrorResponse.unauthorized(
            NotAuthorizedException.class.getSimpleName(), message, null);
      case HttpServletResponse.SC_FORBIDDEN:
        return ErrorResponse.forbidden(message, null);
      case HttpServletResponse.SC_NOT_FOUND:
        return ErrorResponse.notFound(NotFoundException.class.getSimpleName(), message);
      case HttpServletResponse.SC_METHOD_NOT_ALLOWED:
        return ErrorResponse.unsupportedOperation(message);
      default:
        return ErrorResponse.internalError(message);
    }
  }
}
