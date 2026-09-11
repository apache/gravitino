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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.server.web.ServerHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reports errors on the request path as server errors without deciding process lifetime. */
public class ErrorExceptionMapper implements ExceptionMapper<Error> {
  private static final Logger LOG = LoggerFactory.getLogger(ErrorExceptionMapper.class);

  private final ServerHealth health;

  /** Creates a mapper using the shared server health state. */
  public ErrorExceptionMapper() {
    this(ServerHealth.getInstance());
  }

  /**
   * Creates a mapper using the supplied health state.
   *
   * @param health the state to update before constructing an error response
   */
  public ErrorExceptionMapper(ServerHealth health) {
    this.health = health;
  }

  /**
   * Returns a server error response retaining the original error type and complete stack trace.
   *
   * @param error The error raised while processing the request.
   * @return The internal server error response.
   */
  @Override
  public Response toResponse(Error error) {
    health.recordFailure(error);
    String message = "Server error while processing request: " + error;
    LOG.error(message, error);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(ErrorResponse.internalError(error.getClass().getSimpleName(), message, error))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }
}
