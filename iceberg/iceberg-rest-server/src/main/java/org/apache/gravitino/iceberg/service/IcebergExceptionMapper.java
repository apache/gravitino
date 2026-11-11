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
package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.TokenExpiredException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.server.web.ServerHealth;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.glassfish.jersey.server.ParamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Referred from Apache Iceberg's EXCEPTION_ERROR_CODES implementation
// core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java
@Provider
public class IcebergExceptionMapper implements ExceptionMapper<Throwable> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergExceptionMapper.class);

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(BadRequestException.class, 400)
          .put(ValidationException.class, 400)
          .put(IllegalNameIdentifierException.class, 400)
          .put(NamespaceNotEmptyException.class, 409)
          .put(NotAuthorizedException.class, 401)
          .put(UnauthorizedException.class, 401)
          .put(AuthenticationTimeoutException.class, 419)
          .put(TokenExpiredException.class, 419)
          .put(org.apache.gravitino.exceptions.ForbiddenException.class, 403)
          .put(ForbiddenException.class, 403)
          // javax.ws.rs.NotFoundException (JAX-RS, imported above) for an unknown REST path.
          .put(NotFoundException.class, 404)
          // Iceberg NotFoundException is intentionally unmapped (defaults to 500). It can mean a
          // server-side missing file (table metadata/manifest gone from storage), not only a bad
          // client path. Request-supplied metadata-location cases convert to BadRequestException at
          // the call site (e.g. loadTableMetadataFromLocation / registerTable / registerView).
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(NoSuchCatalogException.class, 404)
          .put(NoSuchPlanTaskException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(NoSuchViewException.class, 404)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .put(ServiceFailureException.class, 500)
          .put(ServiceUnavailableException.class, 503)
          .build();

  /**
   * Returns the HTTP status code for the given exception.
   *
   * <ol>
   *   <li>Iceberg / Gravitino business exceptions from {@link #EXCEPTION_ERROR_CODES}
   *   <li>JAX-RS {@link WebApplicationException} that already carries an HTTP status
   *   <li>Unexpected failures default to 500
   * </ol>
   *
   * @param ex the exception
   * @return the HTTP status code
   */
  public static int getErrorCode(Exception ex) {
    // Match the exception's own class first, then its superclasses, so a subclass such as
    // NumberFormatException gets the status of IllegalArgumentException.
    for (Class<?> c = ex.getClass(); c != null; c = c.getSuperclass()) {
      Integer code = EXCEPTION_ERROR_CODES.get(c);
      if (code != null) {
        return code;
      }
    }
    if (ex instanceof WebApplicationException) {
      return ((WebApplicationException) ex).getResponse().getStatus();
    }
    return Status.INTERNAL_SERVER_ERROR.getStatusCode();
  }

  /**
   * Converts a Gravitino or generic exception to the corresponding Iceberg REST spec exception.
   *
   * @param e the original exception
   * @return the equivalent Iceberg exception, or the original if already an Iceberg exception
   */
  public static Exception convertToIcebergException(Exception e) {
    String message = e.getMessage() != null ? e.getMessage() : "";
    if (e instanceof TokenExpiredException) {
      return new AuthenticationTimeoutException("%s", message);
    }
    if (e instanceof UnauthorizedException) {
      return new NotAuthorizedException("%s", message);
    }
    if (e instanceof org.apache.gravitino.exceptions.ForbiddenException) {
      return new ForbiddenException("%s", message);
    }
    if (e instanceof IllegalArgumentException
        || e instanceof IllegalNameIdentifierException
        || e instanceof ValidationException) {
      return new BadRequestException("%s", message);
    }
    if (EXCEPTION_ERROR_CODES.containsKey(e.getClass()) || e instanceof WebApplicationException) {
      return e;
    }
    return new ServiceFailureException("%s", message);
  }

  /**
   * Maps an uncaught throwable to an Iceberg REST error response.
   *
   * @param ex the failure raised while processing the request
   * @return the error response, defaulting to HTTP 500 for unmapped failures
   */
  @Override
  public Response toResponse(Throwable ex) {
    return toRESTResponse(ex);
  }

  public static Response toRESTResponse(Throwable ex) {
    ServerHealth.getInstance().recordFailure(ex);
    ex = toClientFacingException(ex);
    int status =
        ex instanceof Exception
            ? getErrorCode((Exception) ex)
            : Status.INTERNAL_SERVER_ERROR.getStatusCode();
    if (status == Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      LOG.error("Iceberg REST server unexpected failure:", ex);
    } else {
      LOG.info(
          "Iceberg REST server error maybe caused by user request, response http status: {}, exception: {}, exception message: {}",
          status,
          ex.getClass(),
          ex.getMessage());
    }
    return IcebergRESTUtils.errorResponse(ex, status);
  }

  /**
   * Replaces exceptions whose type or message would expose server internals, and client errors that
   * reach the server as framework or storage-SDK exceptions, with the Iceberg exception the REST
   * spec expects for the condition.
   *
   * @param ex the failure raised while processing the request
   * @return the exception to report to the client
   */
  static Throwable toClientFacingException(Throwable ex) {
    JsonProcessingException jsonError = findCause(ex, JsonProcessingException.class);
    if (jsonError != null) {
      return new BadRequestException("%s", describeJsonError(jsonError));
    }
    if (ex instanceof ParamException) {
      return new BadRequestException(
          "Invalid value for parameter '%s'", ((ParamException) ex).getParameterName());
    }
    Throwable storageError = findStorageError(ex);
    if (storageError != null) {
      RuntimeException mapped = toStorageException(storageError);
      return mapped == null ? ex : mapped;
    }
    return ex;
  }

  private static String describeJsonError(JsonProcessingException e) {
    // Iceberg's parsers report invalid input (an unknown update action or requirement type) as
    // IllegalArgumentException or UnsupportedOperationException with a message meant for users.
    for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
      if ((t instanceof IllegalArgumentException || t instanceof UnsupportedOperationException)
          && t.getMessage() != null) {
        return "Invalid request body: " + t.getMessage();
      }
    }
    if (e instanceof JsonMappingException && !((JsonMappingException) e).getPath().isEmpty()) {
      String field =
          ((JsonMappingException) e)
              .getPath().stream()
                  .map(
                      r ->
                          r.getFieldName() != null
                              ? r.getFieldName()
                              : String.valueOf(r.getIndex()))
                  .collect(Collectors.joining("."));
      return "Invalid request body: unexpected value for field '" + field + "'";
    }
    return "Invalid request body: malformed JSON";
  }

  private static Throwable findStorageError(Throwable ex) {
    for (Throwable t = ex; t != null; t = t.getCause()) {
      String name = t.getClass().getName();
      if (name.startsWith("software.amazon.awssdk.")
          || name.startsWith("com.google.cloud.storage.")
          || name.startsWith("com.google.cloud.BaseServiceException")
          || name.startsWith("com.azure.")) {
        return t;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }

  private static RuntimeException toStorageException(Throwable storageError) {
    Integer status = storageStatusCode(storageError);
    String reason = storageErrorCode(storageError);
    String message = storageError.getMessage() == null ? "" : storageError.getMessage();
    // A missing object (for example a deleted metadata file) keeps its existing handling: it is not
    // a problem with the request.
    if (status != null && status == 404 && !"NoSuchBucket".equals(reason)) {
      return null;
    }
    // A bucket that does not exist, a bucket in another region or behind another endpoint (301,
    // 307), a request the storage service rejects as malformed (400), and a request the SDK cannot
    // even build (a location without a bucket) are problems with the location the client asked
    // for. Anything else is the storage service failing. The SDK message is never returned: it
    // carries request ids and SDK details.
    boolean clientLocation =
        status != null && (status == 400 || status == 404 || status == 301 || status == 307);
    if (clientLocation || (status == null && message.startsWith("Unable to marshall request"))) {
      return new BadRequestException(
          "The storage location cannot be used by this catalog (%s%s). Check the location"
              + " and that the catalog's storage configuration can reach it.",
          reason, status == null ? "" : ", HTTP " + status);
    }
    return new ServiceUnavailableException(
        "The catalog's storage service did not complete the request (%s%s)",
        reason, status == null ? "" : ", HTTP " + status);
  }

  private static String storageErrorCode(Throwable storageError) {
    // AWS SDK v2 reports the service error code (for example NoSuchBucket) in awsErrorDetails().
    Object details = invoke(storageError, "awsErrorDetails");
    Object code = details == null ? null : invoke(details, "errorCode");
    if (code instanceof String && !((String) code).isEmpty()) {
      return (String) code;
    }
    return storageError.getClass().getSimpleName().replaceAll("Exception$", "");
  }

  private static Integer storageStatusCode(Throwable storageError) {
    // AWS SDK v2: statusCode(); Google Cloud: getCode(); Azure: getResponse().getStatusCode().
    Integer status = invokeInt(storageError, "statusCode");
    if (status == null) {
      status = invokeInt(storageError, "getCode");
    }
    if (status == null) {
      Object response = invoke(storageError, "getResponse");
      status = response == null ? null : invokeInt(response, "getStatusCode");
    }
    return status != null && status > 0 ? status : null;
  }

  private static Integer invokeInt(Object target, String methodName) {
    Object value = invoke(target, methodName);
    return value instanceof Integer ? (Integer) value : null;
  }

  private static Object invoke(Object target, String methodName) {
    try {
      Method method = target.getClass().getMethod(methodName);
      return method.invoke(target);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return null;
    }
  }

  private static <T extends Throwable> T findCause(Throwable ex, Class<T> type) {
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }
}
