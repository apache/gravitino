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

package org.apache.gravitino.auth.local.web.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.junit.jupiter.api.Test;

public class TestExceptionHandlers {

  @Test
  public void testGetErrorMsgReturnsEmptyStringForNullThrowable() {
    assertEquals("", ExceptionHandlers.BaseExceptionHandler.getErrorMsg(null));
  }

  @Test
  public void testGetErrorMsgStripsNestedExceptionPrefix() {
    RuntimeException exception = new RuntimeException("outer Exception: root cause");

    assertEquals("root cause", ExceptionHandlers.BaseExceptionHandler.getErrorMsg(exception));
  }

  @Test
  public void testBaseExceptionHandlerMapsUnhandledExceptionToInternalError() {
    Response response =
        new ExceptionHandlers.BaseExceptionHandler()
            .handle(OperationType.GET, "alice", "team", new RuntimeException("boom"));
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate object [alice] operation [GET] under [team], reason [boom]",
        errorResponse.getMessage());
  }

  @Test
  public void testHandleUserExceptionMapsForbiddenException() {
    Response response =
        ExceptionHandlers.handleUserException(
            OperationType.ADD, "alice", new ForbiddenException("denied"));
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP user [alice] operation [ADD], reason [denied]",
        errorResponse.getMessage());
  }

  @Test
  public void testHandleGroupExceptionMapsAlreadyExistsException() {
    Response response =
        ExceptionHandlers.handleGroupException(
            OperationType.ADD, "dev", new GroupAlreadyExistsException("group already exists"));
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();

    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(
        "Failed to operate built-in IdP group [dev] operation [ADD], reason [group already exists]",
        errorResponse.getMessage());
  }
}
