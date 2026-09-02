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

import javax.annotation.Nullable;
import javax.annotation.Priority;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.gravitino.server.web.Utils;
import org.glassfish.jersey.server.ParamException;

/**
 * ParamExceptionMapper returns a structured JSON error body when Jersey fails to convert a request
 * parameter (e.g. a typed {@code @PathParam}) into its declared Java type, instead of letting the
 * servlet container fall back to Jetty's default HTML error page.
 *
 * <p>{@link ParamException} is thrown by Jersey itself, before any resource method or other
 * registered {@link ExceptionMapper} runs, so this is the only place such a conversion failure can
 * be intercepted.
 */
@Priority(1)
public class ParamExceptionMapper implements ExceptionMapper<ParamException> {

  @Override
  public Response toResponse(ParamException exception) {
    String message =
        String.format(
            "Invalid value for %s parameter '%s'%s",
            exception.getParameterType().getSimpleName(),
            exception.getParameterName(),
            causeMessage(exception.getCause()));

    if (exception.getResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
      return Utils.notFound(exception.getClass().getSimpleName(), message);
    }
    return Utils.illegalArguments(message);
  }

  private static String causeMessage(@Nullable Throwable cause) {
    return cause == null || cause.getMessage() == null ? "" : ": " + cause.getMessage();
  }
}
