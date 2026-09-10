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
package org.apache.gravitino.server.web.mapper;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests server error responses for errors raised on the request path. */
public class TestErrorExceptionMapper {
  /** Checks that error types and nested causes survive response construction. */
  @Test
  public void testErrorsRetainTypeAndCause() {
    for (Error error :
        new Error[] {
          new OutOfMemoryError("Metaspace"), new StackOverflowError(),
          new NoClassDefFoundError("catalog class"), new AssertionError("assertion")
        }) {
      error.initCause(new IllegalStateException("root cause"));
      try (Response response = new ErrorExceptionMapper().toResponse(error)) {
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
        ErrorResponse entity = (ErrorResponse) response.getEntity();
        Assertions.assertEquals(error.getClass().getSimpleName(), entity.getType());
        Assertions.assertEquals(
            "Server error while processing request: " + error, entity.getMessage());
        String stack = String.join("\n", entity.getStack());
        Assertions.assertTrue(stack.contains(error.toString()));
        Assertions.assertTrue(
            stack.contains("Caused by: java.lang.IllegalStateException: root cause"));
      }
    }
  }
}
