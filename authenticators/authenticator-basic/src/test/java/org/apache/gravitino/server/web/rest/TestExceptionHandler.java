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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

public class TestExceptionHandler {

  @Test
  public void testHandleDelegatesToSubclassImplementation() {
    RuntimeException exception = new RuntimeException("boom");
    RecordingExceptionHandler handler = new RecordingExceptionHandler();

    Response response = handler.handle(IdpOperationType.UPDATE, "alice", "group", exception);

    assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    assertEquals(IdpOperationType.UPDATE, handler.operationType);
    assertEquals("alice", handler.object);
    assertEquals("group", handler.parent);
    assertSame(exception, handler.exception);
  }

  private static class RecordingExceptionHandler extends IdpExceptionHandler {
    private IdpOperationType operationType;
    private String object;
    private String parent;
    private Exception exception;

    @Override
    public Response handle(IdpOperationType op, String object, String parent, Exception e) {
      this.operationType = op;
      this.object = object;
      this.parent = parent;
      this.exception = e;
      return Response.status(Response.Status.ACCEPTED).build();
    }
  }
}
