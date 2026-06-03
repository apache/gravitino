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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST helpers for built-in IdP resources in the {@code idp-basic} plugin. */
public final class IdpRESTUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IdpRESTUtils.class);

  private static final MediaType JSON = MediaType.APPLICATION_JSON_TYPE;

  private IdpRESTUtils() {}

  public static <T> Response ok(T entity) {
    return json(Response.Status.OK, entity);
  }

  public static Response doAs(
      HttpServletRequest httpRequest, PrivilegedExceptionAction<Response> action) throws Exception {
    UserPrincipal principal =
        (UserPrincipal)
            httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);
    return PrincipalUtils.doAs(
        principal == null ? new UserPrincipal(AuthConstants.ANONYMOUS_USER) : principal, action);
  }

  public static Response doAs(
      HttpServletRequest httpRequest,
      PrivilegedExceptionAction<Response> action,
      String resourceType,
      IdpOperationType op,
      String resourceName) {
    try {
      return doAs(httpRequest, action);
    } catch (Exception e) {
      return handleException(resourceType, op, resourceName, e);
    }
  }

  public static Response handleException(
      String resourceType, IdpOperationType op, String name, Exception e) {
    String formatted = StringUtils.isBlank(name) ? "" : " [" + name + "]";
    String errorMsg =
        String.format(
            "Failed to operate built-in IdP %s %s operation [%s], reason [%s]",
            resourceType, formatted, op.name(), e.getMessage());
    LOG.warn(errorMsg, e);
    return toErrorResponse(errorMsg, e);
  }

  public static Response illegalArguments(String message, Throwable throwable) {
    return json(
        Response.Status.BAD_REQUEST,
        ErrorResponse.illegalArguments(
            exceptionType(throwable, "IllegalArgumentException"), message, throwable));
  }

  public static Response notFound(String message, Throwable throwable) {
    return json(
        Response.Status.NOT_FOUND,
        ErrorResponse.notFound(exceptionType(throwable, "NotFoundException"), message, throwable));
  }

  public static Response alreadyExists(String message, Throwable throwable) {
    return json(
        Response.Status.CONFLICT,
        ErrorResponse.alreadyExists(
            exceptionType(throwable, "AlreadyExistsException"), message, throwable));
  }

  public static Response forbidden(String message, Throwable throwable) {
    return json(Response.Status.FORBIDDEN, ErrorResponse.forbidden(message, throwable));
  }

  public static Response unsupportedOperation(String message, Throwable throwable) {
    return json(
        Response.Status.METHOD_NOT_ALLOWED, ErrorResponse.unsupportedOperation(message, throwable));
  }

  public static Response internalError(String message, Throwable throwable) {
    return json(
        Response.Status.INTERNAL_SERVER_ERROR, ErrorResponse.internalError(message, throwable));
  }

  private static Response toErrorResponse(String errorMsg, Exception e) {
    if (e instanceof IllegalArgumentException) {
      return illegalArguments(errorMsg, e);
    }
    if (e instanceof NotFoundException) {
      return notFound(errorMsg, e);
    }
    if (e instanceof AlreadyExistsException) {
      return alreadyExists(errorMsg, e);
    }
    if (e instanceof IllegalStateException || e instanceof UnsupportedOperationException) {
      return unsupportedOperation(errorMsg, e);
    }
    return internalError(errorMsg, e);
  }

  private static Response json(Response.Status status, Object entity) {
    return Response.status(status).entity(entity).type(JSON).build();
  }

  private static String exceptionType(Throwable throwable, String defaultType) {
    return throwable == null ? defaultType : throwable.getClass().getSimpleName();
  }
}
