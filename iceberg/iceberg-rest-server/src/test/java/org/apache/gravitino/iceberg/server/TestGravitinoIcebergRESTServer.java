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
package org.apache.gravitino.iceberg.server;

import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.gravitino.Configs;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.utils.FetchFileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoIcebergRESTServer {

  @TempDir File tempDir;

  @AfterEach
  public void resetBlockUnsafeRemoteUri() {
    FetchFileUtils.setBlockUnsafeRemoteUri(true);
  }

  @Test
  public void testConfigureRemoteUriValidation() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.loadFromMap(
        Collections.singletonMap(Configs.BLOCK_UNSAFE_REMOTE_URI.getKey(), "false"), key -> true);

    GravitinoIcebergRESTServer.configureRemoteUriValidation(serverConfig);

    File destFile = new File(tempDir, "keytab");
    HttpServer server = createLoopbackHttpServer();
    try {
      server.start();
      FetchFileUtils.fetchFileFromUri(
          String.format("http://127.0.0.1:%d/keytab", server.getAddress().getPort()),
          destFile,
          30000,
          null);
      Assertions.assertEquals("keytab", Files.readString(destFile.toPath()));
    } finally {
      server.stop(0);
    }
  }

  private HttpServer createLoopbackHttpServer() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/keytab",
        exchange -> {
          byte[] bytes = "keytab".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
          }
        });
    return server;
  }
}
