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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.server.web.ServerHealth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class TestIdpRESTUtils {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRecordsWrappedOomInRequestHelpers(boolean oom) {
    for (boolean viaDoAs : new boolean[] {true, false}) {
      ServerHealth health = new ServerHealth();
      // A mapped 400 must still record an OOM; internalError alone is insufficient.
      IllegalArgumentException failure =
          new IllegalArgumentException(
              "invalid request",
              oom ? new OutOfMemoryError("Metaspace") : new IllegalStateException("ordinary"));
      try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
        state.when(ServerHealth::getInstance).thenReturn(health);
        try (Response response =
            viaDoAs
                ? IdpRESTUtils.doAs(
                    mock(HttpServletRequest.class),
                    () -> {
                      throw failure;
                    },
                    "group",
                    IdpOperationType.GET,
                    "group")
                : IdpRESTUtils.handleException("group", IdpOperationType.GET, "group", failure)) {
          assertEquals(400, response.getStatus());
          assertEquals(oom, health.hasOutOfMemoryError());
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"direct", "wrapped", "ordinary"})
  void testRecordsOomInInternalError(String kind) {
    ServerHealth health = new ServerHealth();
    Throwable failure =
        kind.equals("direct")
            ? new OutOfMemoryError("Metaspace")
            : kind.equals("wrapped")
                ? new RuntimeException(new OutOfMemoryError("Metaspace"))
                : new IllegalStateException("ordinary");
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      try (Response response = IdpRESTUtils.internalError("failure", failure)) {
        assertEquals(500, response.getStatus());
        assertEquals(!kind.equals("ordinary"), health.hasOutOfMemoryError());
      }
    }
  }

  @Test
  void testUnsupportedOperationReturnsNotImplemented() {
    Response response =
        IdpRESTUtils.handleException(
            "group",
            IdpOperationType.GET,
            "group",
            new UnsupportedOperationException("unsupported"));

    assertEquals(Response.Status.NOT_IMPLEMENTED.getStatusCode(), response.getStatus());
    ErrorResponse error = (ErrorResponse) response.getEntity();
    assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, error.getCode());
  }

  @Test
  void testNonEmptyEntityReturnsConflict() {
    Response response =
        IdpRESTUtils.handleException(
            "group",
            IdpOperationType.REMOVE,
            "group",
            new NonEmptyEntityException("Group is not empty"));

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    ErrorResponse error = (ErrorResponse) response.getEntity();
    assertEquals(ErrorConstants.NON_EMPTY_CODE, error.getCode());
    assertEquals(NonEmptyEntityException.class.getSimpleName(), error.getType());
  }

  @Test
  void testUnexpectedIllegalStateReturnsInternalError() {
    Response response =
        IdpRESTUtils.handleException(
            "group", IdpOperationType.GET, "group", new IllegalStateException("invalid state"));

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    ErrorResponse error = (ErrorResponse) response.getEntity();
    assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, error.getCode());
  }
}
