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
package org.apache.gravitino.maintenance.web;

import java.security.PrivilegedExceptionAction;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.server.web.ServerHealth;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST helpers for table-maintenance resources in the plugin. */
public final class TableMaintenanceRESTUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceRESTUtils.class);

  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  private TableMaintenanceRESTUtils() {}

  /**
   * Returns HTTP 200 with a JSON entity.
   *
   * @param entity response body
   * @param <T> entity type
   * @return OK response
   */
  public static <T> Response ok(T entity) {
    return json(Response.Status.OK, entity);
  }

  /**
   * Runs {@code action} as the authenticated principal.
   *
   * @param httpRequest current request
   * @param action work to run
   * @return the action result
   * @throws Exception when the action fails
   */
  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    return PrincipalUtils.doAs(
        principal == null ? new UserPrincipal(AuthConstants.ANONYMOUS_USER) : principal, action);
  }

  /**
   * Runs {@code action} and maps failures to the same status codes as the built-in IdP REST utils.
   *
   * @param httpRequest current request
   * @param action work to run
   * @param operation short label for logs (e.g. {@code report-before-metrics})
   * @param jobId Gravitino job run id
   * @param metalake metalake name
   * @return success or error response
   */
  public static Response doAs(
      HttpServletRequest httpRequest,
      PrivilegedExceptionAction<Response> action,
      String operation,
      long jobId,
      String metalake) {
    try {
      return doAs(httpRequest, action);
    } catch (Exception e) {
      return handleException(operation, jobId, metalake, e);
    }
  }

  /**
   * Maps a maintenance-action failure to an error response.
   *
   * @param operation short label for logs
   * @param jobId Gravitino job run id
   * @param metalake metalake name
   * @param e failure
   * @return error response
   */
  public static Response handleException(
      String operation, long jobId, String metalake, Exception e) {
    ServerHealth.getInstance().recordFailure(e);
    String errorMsg =
        String.format(
            "Failed to operate maintenance action [%s] operation [%s] under metalake [%s], "
                + "reason [%s]",
            jobId, operation, metalake, e.getMessage());
    LOG.warn(errorMsg, e);
    return toErrorResponse(errorMsg, e);
  }

  private static Response toErrorResponse(String errorMsg, Exception e) {
    if (e instanceof IllegalArgumentException) {
      return illegalArguments(errorMsg, e);
    }
    if (e instanceof ForbiddenException) {
      return forbidden(errorMsg, e);
    }
    if (e instanceof NotFoundException) {
      return notFound(errorMsg, e);
    }
    if (e instanceof AlreadyExistsException) {
      return alreadyExists(errorMsg, e);
    }
    if (e instanceof NonEmptyEntityException) {
      return json(
          Response.Status.CONFLICT,
          ErrorResponse.nonEmpty(e.getClass().getSimpleName(), errorMsg, e));
    }
    if (e instanceof UnsupportedOperationException) {
      return unsupportedOperation(errorMsg, e);
    }
    return internalError(errorMsg, e);
  }

  private static Response illegalArguments(String message, Throwable throwable) {
    return json(
        Response.Status.BAD_REQUEST,
        ErrorResponse.illegalArguments(
            exceptionType(throwable, "IllegalArgumentException"), message, throwable));
  }

  private static Response notFound(String message, Throwable throwable) {
    return json(
        Response.Status.NOT_FOUND,
        ErrorResponse.notFound(exceptionType(throwable, "NotFoundException"), message, throwable));
  }

  private static Response alreadyExists(String message, Throwable throwable) {
    return json(
        Response.Status.CONFLICT,
        ErrorResponse.alreadyExists(
            exceptionType(throwable, "AlreadyExistsException"), message, throwable));
  }

  private static Response forbidden(String message, Throwable throwable) {
    return json(Response.Status.FORBIDDEN, ErrorResponse.forbidden(message, throwable));
  }

  private static Response unsupportedOperation(String message, Throwable throwable) {
    return json(
        Response.Status.NOT_IMPLEMENTED, ErrorResponse.unsupportedOperation(message, throwable));
  }

  private static Response internalError(String message, Throwable throwable) {
    ServerHealth.getInstance().recordFailure(throwable);
    return json(
        Response.Status.INTERNAL_SERVER_ERROR, ErrorResponse.internalError(message, throwable));
  }

  private static Response json(Response.Status status, Object entity) {
    return Response.status(status).entity(entity).type(JSON).build();
  }

  private static String exceptionType(Throwable throwable, String defaultType) {
    return throwable == null ? defaultType : throwable.getClass().getSimpleName();
  }
}
