/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;

/** Builds sanitized Iceberg REST responses for deletion-lifecycle failures. */
public final class IcebergDeletionResponses {

  private IcebergDeletionResponses() {}

  /**
   * Converts a typed lifecycle failure to a stack-free Iceberg REST error.
   *
   * @param exception typed lifecycle failure
   * @return sanitized Iceberg REST error response
   */
  public static Response toResponse(IcebergDeletionException exception) {
    int status;
    switch (exception.outcome()) {
      case NOT_FOUND:
        status = 404;
        break;
      case CONFLICT:
        status = 409;
        break;
      case GONE:
        status = 410;
        break;
      default:
        throw new IllegalStateException("Unknown deletion outcome: " + exception.outcome());
    }
    return Response.status(status)
        .entity(
            IcebergRESTUtils.errorResponse(
                status, IcebergDeletionException.class.getSimpleName(), exception.getMessage()))
        .type(MediaType.APPLICATION_JSON)
        .header(HttpHeaders.CACHE_CONTROL, "private, no-store")
        .build();
  }
}
