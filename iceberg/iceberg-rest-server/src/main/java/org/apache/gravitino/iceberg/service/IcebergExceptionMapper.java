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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Referred from Apache Iceberg's EXCEPTION_ERROR_CODES implementation
// core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java
@Provider
public class IcebergExceptionMapper implements ExceptionMapper<Exception> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergExceptionMapper.class);

  private static final Map<Class<? extends Exception>, Integer> EXCEPTION_ERROR_CODES =
      ImmutableMap.<Class<? extends Exception>, Integer>builder()
          .put(IllegalArgumentException.class, 400)
          .put(ValidationException.class, 400)
          .put(IllegalNameIdentifierException.class, 400)
          .put(NamespaceNotEmptyException.class, 400)
          .put(NotAuthorizedException.class, 401)
          .put(org.apache.gravitino.exceptions.ForbiddenException.class, 403)
          .put(ForbiddenException.class, 403)
          .put(NoSuchNamespaceException.class, 404)
          .put(NoSuchTableException.class, 404)
          .put(NoSuchIcebergTableException.class, 404)
          .put(NoSuchCatalogException.class, 404)
          .put(UnsupportedOperationException.class, 406)
          .put(NoSuchViewException.class, 404)
          .put(AlreadyExistsException.class, 409)
          .put(CommitFailedException.class, 409)
          .put(UnprocessableEntityException.class, 422)
          .put(CommitStateUnknownException.class, 500)
          .put(ServiceUnavailableException.class, 503)
          .build();

  @Override
  public Response toResponse(Exception ex) {
    return toRESTResponse(ex);
  }

  public static Response toRESTResponse(Exception ex) {
    int status =
        EXCEPTION_ERROR_CODES.getOrDefault(
            ex.getClass(), Status.INTERNAL_SERVER_ERROR.getStatusCode());
    if (status == Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      LOG.warn("Iceberg REST server unexpected exception:", ex);
    } else {
      LOG.info(
          "Iceberg REST server error maybe caused by user request, response http status: {}, exception: {}, exception message: {}",
          status,
          ex.getClass(),
          ex.getMessage());
    }
    return IcebergRestUtils.errorResponse(ex, status);
  }
}
