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
package org.apache.gravitino.integration.test.container;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the Kafka readiness probe against a local socket; nothing here starts a container. */
public class TestKafkaContainer {

  @Test
  void testProbeSendsApiVersionsAndAcceptsTheMatchingAnswer() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      CompletableFuture<String> request =
          CompletableFuture.supplyAsync(() -> answer(server, /* echoCorrelationId= */ true));

      Assertions.assertTrue(KafkaContainer.answersApiVersions("127.0.0.1", server.getLocalPort()));
      Assertions.assertEquals("18/0/gravitino-it", request.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void testProbeRejectsAnAnswerToAnotherRequest() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      CompletableFuture.supplyAsync(() -> answer(server, /* echoCorrelationId= */ false));

      Assertions.assertFalse(KafkaContainer.answersApiVersions("127.0.0.1", server.getLocalPort()));
    }
  }

  @Test
  void testProbeFailsWhenNothingListens() throws IOException {
    int port;
    try (ServerSocket server = new ServerSocket(0)) {
      port = server.getLocalPort();
    }

    Assertions.assertFalse(KafkaContainer.answersApiVersions("127.0.0.1", port));
  }

  // Reads one request and answers with a bare response header; returns "key/version/clientId".
  private static String answer(ServerSocket server, boolean echoCorrelationId) {
    try (Socket socket = server.accept()) {
      DataInputStream in = new DataInputStream(socket.getInputStream());
      in.readInt();
      short apiKey = in.readShort();
      short apiVersion = in.readShort();
      int correlationId = in.readInt();
      byte[] clientId = new byte[in.readShort()];
      in.readFully(clientId);

      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      out.writeInt(4);
      out.writeInt(echoCorrelationId ? correlationId : correlationId + 1);
      out.flush();
      return apiKey + "/" + apiVersion + "/" + new String(clientId, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
