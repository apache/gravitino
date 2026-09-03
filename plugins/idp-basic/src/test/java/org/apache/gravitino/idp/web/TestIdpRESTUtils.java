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

import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.junit.jupiter.api.Test;

class TestIdpRESTUtils {

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
