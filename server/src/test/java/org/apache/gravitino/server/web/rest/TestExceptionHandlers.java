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
package org.apache.gravitino.server.web.rest;

import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptionHandlers {

  @Test
  public void testGetErrorMsg() {
    Exception e1 = new Exception("test1");
    Exception e2 = new Exception("test2", e1);
    Exception e3 = new Exception(e1);
    Exception e4 = new Exception();
    Exception e5 = new Exception(e2);
    Exception e6 = null;

    String msg1 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e1);
    Assertions.assertEquals("test1", msg1);

    String msg2 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e2);
    Assertions.assertEquals("test2", msg2);

    String msg3 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e3);
    Assertions.assertEquals("test1", msg3);

    String msg4 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e4);
    Assertions.assertEquals("", msg4);

    String msg5 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e5);
    Assertions.assertEquals("test2", msg5);

    String msg6 = ExceptionHandlers.BaseExceptionHandler.getErrorMsg(e6);
    Assertions.assertEquals("", msg6);
  }

  @Test
  public void testHandleTestConnectionExceptionShouldReturnErrorStatus() {
    Response illegalArgumentsResponse =
        ExceptionHandlers.handleTestConnectionException(
            new IllegalArgumentException("invalid properties"));
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), illegalArgumentsResponse.getStatus());

    Response notFoundResponse =
        ExceptionHandlers.handleTestConnectionException(new NotFoundException("catalog not found"));
    Assertions.assertEquals(
        Response.Status.NOT_FOUND.getStatusCode(), notFoundResponse.getStatus());

    Response alreadyExistsResponse =
        ExceptionHandlers.handleTestConnectionException(
            new AlreadyExistsException("catalog already exists"));
    Assertions.assertEquals(
        Response.Status.CONFLICT.getStatusCode(), alreadyExistsResponse.getStatus());

    Response notInUseResponse =
        ExceptionHandlers.handleTestConnectionException(new NotInUseException("catalog disabled"));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), notInUseResponse.getStatus());

    Response connectionFailedResponse =
        ExceptionHandlers.handleTestConnectionException(
            new ConnectionFailedException("connection failed"));
    Assertions.assertEquals(
        Response.Status.BAD_GATEWAY.getStatusCode(), connectionFailedResponse.getStatus());

    Response fallbackResponse =
        ExceptionHandlers.handleTestConnectionException(new RuntimeException("unexpected"));
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), fallbackResponse.getStatus());
  }
}
