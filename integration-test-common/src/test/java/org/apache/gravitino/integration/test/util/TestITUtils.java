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
package org.apache.gravitino.integration.test.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestITUtils {
  @Test
  public void testIsCiEnvironmentByCI() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("CI", "true")));
  }

  @Test
  public void testIsCiEnvironmentByGitHubActions() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("GITHUB_ACTIONS", "true")));
  }

  @Test
  public void testIsCiEnvironmentCaseInsensitive() {
    Assertions.assertTrue(ITUtils.isCiEnvironment(Map.of("CI", "TRUE")));
  }

  @Test
  public void testIsCiEnvironmentFalse() {
    Assertions.assertFalse(ITUtils.isCiEnvironment(Map.of()));
    Assertions.assertFalse(ITUtils.isCiEnvironment(Map.of("CI", "false")));
  }

  @Test
  void testCheckServerPortIsFreeAcceptsAnUnusedPort() throws IOException {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    Assertions.assertDoesNotThrow(() -> ITUtils.checkServerPortIsFree("localhost", port));
  }

  @Test
  void testCheckServerPortIsFreeRejectsAPortSomethingElseHolds() throws IOException {
    // A server left behind by an earlier run keeps answering on this port. Starting a suite against
    // it runs the tests against a stranger's configuration, so refuse before the launch rather than
    // after a readiness probe the leftover satisfies.
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();

      IllegalStateException e =
          Assertions.assertThrows(
              IllegalStateException.class, () -> ITUtils.checkServerPortIsFree("localhost", port));

      Assertions.assertTrue(
          e.getMessage().contains(String.valueOf(port)),
          "the message has to name the port so the cause is actionable: " + e.getMessage());
    }
  }
}
