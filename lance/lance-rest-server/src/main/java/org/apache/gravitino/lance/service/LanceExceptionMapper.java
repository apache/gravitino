/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.server.web.ServerHealth;
import org.glassfish.jersey.server.ParamException;
import org.lance.namespace.errors.ConcurrentModificationException;
import org.lance.namespace.errors.InternalException;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotEmptyException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.PermissionDeniedException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.errors.ThrottlingException;
import org.lance.namespace.errors.UnauthenticatedException;
import org.lance.namespace.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class LanceExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceExceptionMapper.class);

  // Set once at service startup. Only Lance REST reads this flag, and its classes are packaged
  // outside the main server's classpath, so each service loads its own copy.
  private static volatile boolean includeErrorStackTrace =
      JettyServerConfig.INCLUDE_ERROR_STACK_TRACE.getDefaultValue();

  @Context private UriInfo uriInfo;
  @Context private Request request;

  /**
   * Sets whether error details built by {@link #errorDetail(Throwable)} include the exception's
   * stack trace. Called once when the service starts.
   *
   * @param include whether to include stack traces
   */
  public static void setIncludeErrorStackTrace(boolean include) {
    includeErrorStackTrace = include;
  }

  /**
   * Returns the error detail for a Lance error response.
   *
   * @param ex the failure to describe
   * @return the stack trace of {@code ex}, or an empty string when stack traces are disabled
   */
  public static String errorDetail(Throwable ex) {
    return includeErrorStackTrace ? getStackTrace(ex) : "";
  }

  public static Response toRESTResponse(String instance, Throwable ex) {
    ServerHealth.getInstance().recordFailure(ex);
    if (ex instanceof WebApplicationException webException) {
      return handleWebApplicationException(instance, webException);
    }
    LanceNamespaceException lanceException =
        ex instanceof LanceNamespaceException
            ? (LanceNamespaceException) ex
            : toLanceNamespaceException(instance, ex);

    if (toHttpStatus(lanceException) >= 500) {
      LOG.error("Operate Lance instance with id '{}' failed", instance, ex);
    } else {
      LOG.info(
          "Operate Lance instance with id '{}' got exception: {}",
          instance,
          lanceException.getMessage());
    }

    return handleLanceNamespaceException(lanceException);
  }

  /** {@inheritDoc} */
  @Override
  public Response toResponse(Throwable ex) {
    String instance = "";
    if (isRouteMiss(ex) && request != null && uriInfo != null) {
      instance = request.getMethod() + " " + uriInfo.getPath(false);
    }
    return toRESTResponse(instance, ex);
  }

  private static boolean isRouteMiss(Throwable ex) {
    return ex instanceof WebApplicationException webException
        && !(ex instanceof ParamException)
        && webException.getResponse().getStatus() == Response.Status.NOT_FOUND.getStatusCode();
  }

  private static Response handleWebApplicationException(
      String instance, WebApplicationException exception) {
    int status = exception.getResponse().getStatus();
    String message = exception.getMessage();
    boolean routeMiss = isRouteMiss(exception);
    if (exception instanceof ParamException parameterException) {
      // Jersey can use 404 for parameter conversion failures, but these are invalid input.
      status = Response.Status.BAD_REQUEST.getStatusCode();
      message = "Invalid value for parameter '" + parameterException.getParameterName() + "'";
    } else if (routeMiss) {
      // Unmatched routes include unimplemented Namespace operations. Code 0 / HTTP 406 lets
      // Lance clients recognize Unsupported and fall back; domain not-found errors stay 404.
      status = Response.Status.NOT_ACCEPTABLE.getStatusCode();
      message = "Unsupported Lance REST operation" + (instance.isEmpty() ? "" : ": " + instance);
    }
    LanceNamespaceException error;
    if (routeMiss) {
      error = new org.lance.namespace.errors.UnsupportedOperationException(message, "", instance);
    } else if (status >= 400 && status < 500) {
      error =
          switch (status) {
            case 401 -> new UnauthenticatedException(message, "", instance);
            case 403 -> new PermissionDeniedException(message, "", instance);
            case 409 -> new ConcurrentModificationException(message, "", instance);
            case 429 -> new ThrottlingException(message, "", instance);
            default -> new InvalidInputException(message, "", instance);
          };
    } else {
      LOG.error("Lance REST framework failure:", exception);
      error = new InternalException("Internal server error", "", instance);
    }
    // Keep protocol headers such as Allow, WWW-Authenticate and Retry-After, while replacing any
    // framework entity with the Lance error envelope. Never expose a framework stack or 5xx detail.
    Response.ResponseBuilder response = Response.fromResponse(exception.getResponse());
    // The old entity's framing, representation metadata and validators do not describe our JSON.
    for (String header :
        new String[] {
          "Content-Length", "Content-Encoding", "Content-Language", "Content-Location",
          "Content-Range", "Content-Disposition", "Transfer-Encoding", "ETag",
          "Last-Modified", "Content-MD5", "Digest"
        }) {
      response.header(header, null);
    }
    return response
        .status(status)
        .entity(errorResponse(error))
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }

  private static LanceNamespaceException toLanceNamespaceException(String instance, Throwable ex) {
    if (ex instanceof ForbiddenException) {
      return new PermissionDeniedException(ex.getMessage(), "", instance);

    } else if (ex instanceof UnauthorizedException) {
      return new UnauthenticatedException(ex.getMessage(), "", instance);

    } else if (ex instanceof NoSuchTableException) {
      return new TableNotFoundException(ex.getMessage(), errorDetail(ex), instance);

    } else if (ex instanceof NotFoundException) {
      return new NamespaceNotFoundException(ex.getMessage(), errorDetail(ex), instance);

    } else if (ex instanceof IllegalArgumentException) {
      // Validation failures never expose generated stacks, independent of the diagnostic setting.
      return new InvalidInputException(ex.getMessage(), "", instance);

    } else if (ex instanceof org.apache.gravitino.exceptions.TableAlreadyExistsException) {
      return new TableAlreadyExistsException(ex.getMessage(), errorDetail(ex), instance);

    } else if (ex instanceof UnsupportedOperationException) {
      return new org.lance.namespace.errors.UnsupportedOperationException(
          ex.getMessage(), errorDetail(ex), instance);

    } else {
      return new InternalException("Internal server error", "", instance);
    }
  }

  private static int toHttpStatus(LanceNamespaceException exception) {
    if (exception instanceof NamespaceNotFoundException
        || exception instanceof TableNotFoundException) {
      return Response.Status.NOT_FOUND.getStatusCode();
    }

    if (exception instanceof NamespaceAlreadyExistsException
        || exception instanceof TableAlreadyExistsException
        || exception instanceof ConcurrentModificationException) {
      return Response.Status.CONFLICT.getStatusCode();
    }

    if (exception instanceof org.lance.namespace.errors.UnsupportedOperationException) {
      return Response.Status.NOT_ACCEPTABLE.getStatusCode();
    }

    if (exception instanceof PermissionDeniedException) {
      return Response.Status.FORBIDDEN.getStatusCode();
    }

    if (exception instanceof UnauthenticatedException) {
      return Response.Status.UNAUTHORIZED.getStatusCode();
    }

    if (exception instanceof ThrottlingException) {
      return 429;
    }

    if (exception instanceof ServiceUnavailableException) {
      return Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
    }

    if (exception instanceof InvalidInputException
        || exception instanceof NamespaceNotEmptyException) {
      return Response.Status.BAD_REQUEST.getStatusCode();
    }

    return Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
  }

  private static Response handleLanceNamespaceException(LanceNamespaceException ex) {
    return Response.status(toHttpStatus(ex)).entity(errorResponse(ex)).build();
  }

  private static ErrorResponse errorResponse(LanceNamespaceException ex) {
    ErrorResponse errResp = new ErrorResponse();
    errResp.setCode(ex.getCode());
    errResp.setError(ex.getMessage());
    errResp.setDetail(ex.getDetail());
    errResp.setInstance(ex.getInstance());
    return errResp;
  }
}
