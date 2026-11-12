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
package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests recording errors before resource-level exception handling hides them. */
class TestUtilsOutOfMemory {
  @Test
  void doAsRecordsAndRethrowsTheOriginalError() {
    ServerHealth health = new ServerHealth();
    OutOfMemoryError failure = new OutOfMemoryError("Metaspace");
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      assertSame(
          failure,
          assertThrows(
              OutOfMemoryError.class,
              () ->
                  Utils.doAs(
                      mock(HttpServletRequest.class),
                      () -> {
                        throw failure;
                      })));
      assertTrue(health.hasOutOfMemoryError());
    }
  }

  @Test
  void doAsRecordsWrappedFailuresBeforeTheyAreMappedToResponses() {
    ServerHealth health = new ServerHealth();
    RuntimeException failure = new RuntimeException(new OutOfMemoryError("Java heap space"));
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      assertThrows(
          Exception.class,
          () ->
              Utils.doAs(
                  mock(HttpServletRequest.class),
                  () -> {
                    throw failure;
                  }));
      assertTrue(health.hasOutOfMemoryError());
    }
  }

  @Test
  void internalErrorRecordsWrappedOomButNotOrdinaryServerErrors() {
    ServerHealth health = new ServerHealth();
    try (MockedStatic<ServerHealth> state = mockStatic(ServerHealth.class)) {
      state.when(ServerHealth::getInstance).thenReturn(health);
      try (Response response = Utils.internalError("unavailable", new IllegalStateException())) {
        assertEquals(500, response.getStatus());
        assertFalse(health.hasOutOfMemoryError());
      }
      try (Response response =
          Utils.internalError("failed", new RuntimeException(new OutOfMemoryError()))) {
        assertEquals(500, response.getStatus());
        assertTrue(health.hasOutOfMemoryError());
      }
    }
  }
}
