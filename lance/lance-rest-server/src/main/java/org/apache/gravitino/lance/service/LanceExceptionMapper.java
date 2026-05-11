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

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NotFoundException;
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
import org.lance.namespace.errors.UnauthenticatedException;
import org.lance.namespace.model.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class LanceExceptionMapper implements ExceptionMapper<Exception> {

  private static final Logger LOG = LoggerFactory.getLogger(LanceExceptionMapper.class);

  public static Response toRESTResponse(String instance, Exception ex) {
    LanceNamespaceException lanceException =
        ex instanceof LanceNamespaceException
            ? (LanceNamespaceException) ex
            : toLanceNamespaceException(instance, ex);

    LOG.error(
        "Operate Lance instance with id '{}' got exception: {}",
        instance,
        lanceException.getMessage());

    return handleLanceNamespaceException(lanceException);
  }

  @Override
  public Response toResponse(Exception ex) {
    return toRESTResponse("", ex);
  }

  private static LanceNamespaceException toLanceNamespaceException(String instance, Exception ex) {
    if (ex instanceof NoSuchTableException) {
      return new TableNotFoundException(ex.getMessage(), getStackTrace(ex), instance);

    } else if (ex instanceof NotFoundException) {
      return new NamespaceNotFoundException(ex.getMessage(), getStackTrace(ex), instance);

    } else if (ex instanceof IllegalArgumentException) {
      return new InvalidInputException(ex.getMessage(), getStackTrace(ex), instance);

    } else if (ex instanceof org.apache.gravitino.exceptions.TableAlreadyExistsException) {
      return new TableAlreadyExistsException(ex.getMessage(), getStackTrace(ex), instance);

    } else if (ex instanceof UnsupportedOperationException) {
      return new org.lance.namespace.errors.UnsupportedOperationException(
          ex.getMessage(), getStackTrace(ex), instance);

    } else {
      LOG.warn("Lance REST server unexpected exception:", ex);
      return new InternalException(ex.getMessage(), getStackTrace(ex), instance);
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
    ErrorResponse errResp = new ErrorResponse();
    errResp.setCode(ex.getCode());
    errResp.setError(ex.getMessage());
    errResp.setDetail(ex.getDetail());
    errResp.setInstance(ex.getInstance());
    return Response.status(toHttpStatus(ex)).entity(errResp).build();
  }
}
