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
package org.apache.gravitino.server.web.mapper;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.eclipse.jetty.http.HttpStatus;

/**
 * WebApplicationExceptionMapper returns a structured JSON error body for any {@link
 * WebApplicationException} that JAX-RS/Jersey raises itself before a resource method runs (e.g. a
 * wrong HTTP method or an unacceptable {@code Accept}/{@code Content-Type} header), instead of
 * letting the servlet container fall back to Jetty's default HTML error page.
 *
 * <p>{@link org.glassfish.jersey.server.ParamException} and {@link javax.ws.rs.NotFoundException}
 * are subtypes of {@link WebApplicationException} with their own, more specific mappers ({@link
 * ParamExceptionMapper}, {@link NotFoundExceptionMapper}); JAX-RS always prefers the mapper
 * registered for the nearest type in the exception's class hierarchy, so this mapper only applies
 * to every other case in the family.
 */
@Priority(1)
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {

  @Override
  public Response toResponse(WebApplicationException exception) {
    int status = exception.getResponse().getStatus();
    String message =
        StringUtils.isBlank(exception.getMessage())
            ? HttpStatus.getMessage(status)
            : exception.getMessage();

    return Response.fromResponse(exception.getResponse())
        .entity(toErrorResponse(status, message))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  private static ErrorResponse toErrorResponse(int status, String message) {
    switch (status) {
      case HttpServletResponse.SC_BAD_REQUEST:
        return ErrorResponse.illegalArguments(message);
      case HttpServletResponse.SC_FORBIDDEN:
        return ErrorResponse.forbidden(message, null);
      case HttpServletResponse.SC_METHOD_NOT_ALLOWED:
        return ErrorResponse.unsupportedOperation(message);
      default:
        return ErrorResponse.restError(message);
    }
  }
}
