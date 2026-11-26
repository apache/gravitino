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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.paimon.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.paimon.RESTService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Verifies Paimon 1.2 client compatibility against the current REST service. */
public class TestPaimon12Compatibility {

  private static final String TOKEN = "paimon12-client-test";

  @TempDir Path directory;

  @Test
  void testPaimon12ClientCreatesAndWritesTables() throws Exception {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    String warehouse = directory.resolve("warehouse").toUri().toString();
    RESTService service = new RESTService();
    service.serviceInit(
        Map.of(
            "host",
            "127.0.0.1",
            "httpPort",
            String.valueOf(port),
            "catalog-backend",
            "filesystem",
            "warehouse",
            warehouse,
            "token",
            TOKEN),
        false);
    try {
      service.serviceStart();
      runClient("http://127.0.0.1:" + port + "/paimon/", warehouse);
    } finally {
      service.serviceStop();
    }
  }

  private void runClient(String uri, String warehouse) throws Exception {
    Path log = directory.resolve("client.log");
    ProcessBuilder client =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("paimon.test.client12.classpath"),
                "org.apache.gravitino.paimon.compatibility.Paimon12TestClient",
                uri,
                warehouse)
            .redirectErrorStream(true)
            .redirectOutput(log.toFile());
    client.environment().put("PAIMON_COMPATIBILITY_TEST_TOKEN", TOKEN);

    Process process = client.start();
    try {
      assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Paimon 1.2 client timed out");
      assertEquals(0, process.exitValue(), Files.readString(log));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
      }
    }
  }
}
