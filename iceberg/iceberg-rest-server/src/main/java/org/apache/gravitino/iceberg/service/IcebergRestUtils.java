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
package org.apache.gravitino.iceberg.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.iceberg.rest.responses.ErrorResponse;

public class IcebergRestUtils {

  private IcebergRestUtils() {}

  public static <T> Response ok(T t) {
    return Response.status(Response.Status.OK).entity(t).type(MediaType.APPLICATION_JSON).build();
  }

  public static Response okWithoutContent() {
    return Response.status(Response.Status.OK).build();
  }

  public static Response noContent() {
    return Response.status(Status.NO_CONTENT).build();
  }

  public static Response notExists() {
    return Response.status(Status.NOT_FOUND).build();
  }

  public static Response errorResponse(Exception ex, int httpStatus) {
    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(httpStatus)
            .withType(ex.getClass().getSimpleName())
            .withMessage(ex.getMessage())
            .withStackTrace(ex)
            .build();
    return Response.status(httpStatus)
        .entity(errorResponse)
        .type(MediaType.APPLICATION_JSON)
        .build();
  }

  public static Instant calculateNewTimestamp(Instant currentTimestamp, int hours) {
    LocalDateTime currentDateTime =
        LocalDateTime.ofInstant(currentTimestamp, ZoneId.systemDefault());
    LocalDateTime nextHourDateTime;
    if (hours > 0) {
      nextHourDateTime = currentDateTime.plusHours(hours);
    } else {
      nextHourDateTime = currentDateTime.minusHours(-hours);
    }
    return nextHourDateTime.atZone(ZoneId.systemDefault()).toInstant();
  }
}
