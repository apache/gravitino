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
package org.apache.gravitino.lance.service;

import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.model.ErrorResponse;

/** Verifies backend authentication failures retain their protocol status without stack traces. */
public class TestLanceExceptionMapper {

  /** Verifies backend authorization failures use the Lance forbidden response. */
  @Test
  public void testBackendForbidden() {
    assertAuthenticationError(new ForbiddenException("Access denied"), 403);
  }

  /** Verifies backend authentication failures use the Lance unauthenticated response. */
  @Test
  public void testBackendUnauthorized() {
    assertAuthenticationError(new UnauthorizedException("Invalid credentials"), 401);
  }

  /** Verifies unexpected exceptions do not expose internal details in the response. */
  @Test
  public void testInternalFailureDoesNotExposeException() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "catalog.schema.table", new RuntimeException("private-backend-detail"))) {
      Assertions.assertEquals(500, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals("Internal server error", error.getError());
      Assertions.assertEquals("", error.getDetail());
    }
  }

  /** Verifies intentional protocol validation details remain available to callers. */
  @Test
  public void testProtocolValidationDetailsArePreserved() {
    try (Response response =
        LanceExceptionMapper.toRESTResponse(
            "table",
            new InvalidInputException("Invalid field", "field must be positive", "table"))) {
      Assertions.assertEquals(400, response.getStatus());
      Assertions.assertEquals(
          "field must be positive", ((ErrorResponse) response.getEntity()).getDetail());
    }
  }

  private void assertAuthenticationError(Exception exception, int status) {
    try (Response response = LanceExceptionMapper.toRESTResponse("catalog", exception)) {
      Assertions.assertEquals(status, response.getStatus());
      ErrorResponse error = (ErrorResponse) response.getEntity();
      Assertions.assertEquals(exception.getMessage(), error.getError());
      Assertions.assertEquals("", error.getDetail());
      Assertions.assertEquals("catalog", error.getInstance());
    }
  }
}
