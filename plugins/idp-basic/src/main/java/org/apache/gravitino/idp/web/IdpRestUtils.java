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
package org.apache.gravitino.idp.web;

import java.security.PrivilegedExceptionAction;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.utils.PrincipalUtils;

/** REST helpers for built-in IdP resources in the {@code idp-basic} plugin. */
public final class IdpRestUtils {

  private IdpRestUtils() {}

  /**
   * Builds a successful REST response.
   *
   * @param entity The response entity.
   * @param <T> The entity type.
   * @return The REST response.
   */
  public static <T> Response ok(T entity) {
    return Response.status(Response.Status.OK)
        .entity(entity)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Executes an action under the authenticated principal from the request.
   *
   * @param httpRequest The HTTP request.
   * @param action The action to execute.
   * @return The REST response.
   * @throws Exception If the action fails.
   */
  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    if (principal == null) {
      principal = new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }
    return PrincipalUtils.doAs(principal, action);
  }

  /**
   * Builds a bad request response.
   *
   * @param message The error message.
   * @param throwable The cause.
   * @return The REST response.
   */
  public static Response illegalArguments(String message, Throwable throwable) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(
            ErrorResponse.illegalArguments(
                throwable.getClass().getSimpleName(), message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Builds a not found response.
   *
   * @param message The error message.
   * @param throwable The cause.
   * @return The REST response.
   */
  public static Response notFound(String message, Throwable throwable) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(ErrorResponse.notFound(throwable.getClass().getSimpleName(), message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Builds a conflict response.
   *
   * @param message The error message.
   * @param throwable The cause.
   * @return The REST response.
   */
  public static Response alreadyExists(String message, Throwable throwable) {
    return Response.status(Response.Status.CONFLICT)
        .entity(
            ErrorResponse.alreadyExists(throwable.getClass().getSimpleName(), message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Builds a method not allowed response.
   *
   * @param message The error message.
   * @param throwable The cause.
   * @return The REST response.
   */
  public static Response unsupportedOperation(String message, Throwable throwable) {
    return Response.status(Response.Status.METHOD_NOT_ALLOWED)
        .entity(ErrorResponse.unsupportedOperation(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  /**
   * Builds an internal error response.
   *
   * @param message The error message.
   * @param throwable The cause.
   * @return The REST response.
   */
  public static Response internalError(String message, Throwable throwable) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(ErrorResponse.internalError(message, throwable))
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
