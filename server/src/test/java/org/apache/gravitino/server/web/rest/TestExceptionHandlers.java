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

<<<<<<< HEAD
=======
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
>>>>>>> 8e41cedff ([#12879] fix(server): return accurate HTTP statuses for unsupported operations (#12880))
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
<<<<<<< HEAD
=======

  @Test
  public void testOptimisticLockConflictReturnsConflict() {
    Response response =
        ExceptionHandlers.handleTableException(
            OperationType.ALTER,
            "table",
            "schema",
            new OptimisticLockException("The table was modified concurrently"));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    Assertions.assertEquals(ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE, errorResponse.getCode());
    Assertions.assertEquals(OptimisticLockException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  void testUnsupportedOperationReturnsNotImplemented() {
    UnsupportedOperationException exception =
        new UnsupportedOperationException("Operation is not supported");
    List<Response> responses =
        List.of(
            ExceptionHandlers.handleTableException(
                OperationType.ALTER, "table", "schema", exception),
            ExceptionHandlers.handlePolicyException(
                OperationType.LIST, "policy", "metalake", exception),
            new ExceptionHandlers.BaseExceptionHandler()
                .handle(OperationType.LIST, "object", "parent", exception));

    responses.forEach(
        response -> {
          Assertions.assertEquals(
              Response.Status.NOT_IMPLEMENTED.getStatusCode(), response.getStatus());
          ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
          Assertions.assertEquals(
              ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResponse.getCode());
          Assertions.assertEquals(
              UnsupportedOperationException.class.getSimpleName(), errorResponse.getType());
        });
  }

  @Test
  void testUnmodifiableOperationReturnsConflict() {
    UnmodifiableStatisticException exception =
        new UnmodifiableStatisticException("Statistic is unmodifiable");
    List<Response> responses =
        List.of(
            ExceptionHandlers.handleStatisticException(
                OperationType.ALTER, "statistic", "table", exception),
            ExceptionHandlers.handlePartitionStatsException(
                OperationType.ALTER, "partition", "table", exception),
            new ExceptionHandlers.BaseExceptionHandler()
                .handle(OperationType.ALTER, "statistic", "table", exception));

    responses.forEach(
        response -> {
          Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
          ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
          Assertions.assertEquals(
              ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResponse.getCode());
          Assertions.assertEquals(
              UnmodifiableStatisticException.class.getSimpleName(), errorResponse.getType());
        });
  }
>>>>>>> 8e41cedff ([#12879] fix(server): return accurate HTTP statuses for unsupported operations (#12880))
}
