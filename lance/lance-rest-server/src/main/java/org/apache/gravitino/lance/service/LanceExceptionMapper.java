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

import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.model.ErrorResponse;
import java.util.Optional;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.exceptions.NotFoundException;
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

    return handleLanceNamespaceException(lanceException);
  }

  @Override
  public Response toResponse(Exception ex) {
    return toRESTResponse("", ex);
  }

  private static LanceNamespaceException toLanceNamespaceException(String instance, Exception ex) {
    if (ex instanceof NotFoundException) {
      return LanceNamespaceException.notFound(
          ex.getMessage(), ex.getClass().getSimpleName(), instance, getStackTrace(ex));

    } else if (ex instanceof IllegalArgumentException) {
      return LanceNamespaceException.badRequest(
          ex.getMessage(), ex.getClass().getSimpleName(), instance, getStackTrace(ex));

    } else if (ex instanceof UnsupportedOperationException) {
      return LanceNamespaceException.unsupportedOperation(
          ex.getMessage(), ex.getClass().getSimpleName(), instance, getStackTrace(ex));

    } else {
      LOG.warn("Lance REST server unexpected exception:", ex);
      return LanceNamespaceException.serverError(
          ex.getMessage(), ex.getClass().getSimpleName(), instance, getStackTrace(ex));
    }
  }

  // Referred from lance-namespace-adapter's LanceNamespaces exception handling
  // com.lancedb.lance.namespace.adapter.GlobalExceptionHandler
  private static Response handleLanceNamespaceException(LanceNamespaceException ex) {
    ErrorResponse errResp = new ErrorResponse();
    Optional<ErrorResponse> errorResponse = ex.getErrorResponse();
    if (errorResponse.isPresent() && errorResponse.get().getCode() != null) {
      errResp = errorResponse.get();

    } else {
      // Transform error info into ErrorResponse
      errResp.setCode(ex.getCode());
      errResp.setError(ex.getMessage());
    }

    return Response.status(errResp.getCode()).entity(errResp).build();
  }
}
