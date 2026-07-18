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
package org.apache.gravitino.s3.credential;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.s3.credential.webidentity.WebIdentityTokenSourceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestAwsIrsaCredentialGenerator {

  private HttpServer server;
  private AtomicReference<String> requestBody;
  private AtomicInteger requestCount;

  @BeforeEach
  void setUp() throws IOException {
    requestBody = new AtomicReference<>();
    requestCount = new AtomicInteger();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handleAssumeRoleWithWebIdentity);
    server.start();
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void basicModeUsesConfiguredTokenSourceWithoutSessionPolicy(@TempDir Path dir)
      throws IOException {
    try (AwsIrsaCredentialGenerator generator = new AwsIrsaCredentialGenerator()) {
      generator.initialize(createProperties(dir));

      AwsIrsaCredential credential = generator.generate(() -> "test-user");

      assertEquals("access-key", credential.accessKeyId());
      assertEquals("secret-key", credential.secretAccessKey());
      assertEquals("session-token", credential.sessionToken());
    }

    String body = URLDecoder.decode(requestBody.get(), StandardCharsets.UTF_8.name());
    assertTrue(body.contains("WebIdentityToken=configured-token"));
    assertTrue(body.contains("RoleSessionName=gravitino_irsa_session_test-user"));
    assertFalse(body.contains("Policy="));
  }

  @Test
  void basicModeReusesCachedStsCredentials(@TempDir Path dir) throws IOException {
    try (AwsIrsaCredentialGenerator generator = new AwsIrsaCredentialGenerator()) {
      generator.initialize(createProperties(dir));

      generator.generate(() -> "test-user");
      generator.generate(() -> "test-user");
    }

    assertEquals(1, requestCount.get());
  }

  @Test
  void minioRoleArnAcceptedWhenCustomStsEndpointConfigured(@TempDir Path dir) throws IOException {
    Map<String, String> properties = createProperties(dir);
    properties.put("s3-role-arn", "arn:minio:iam:::role/test-role");

    try (AwsIrsaCredentialGenerator generator = new AwsIrsaCredentialGenerator()) {
      generator.initialize(properties);

      AwsIrsaCredential credential = generator.generate(() -> "test-user");

      assertEquals("access-key", credential.accessKeyId());
      assertEquals("secret-key", credential.secretAccessKey());
      assertEquals("session-token", credential.sessionToken());
    }
  }

  @Test
  void minioRoleArnRejectedWhenStsEndpointMissing(@TempDir Path dir) throws IOException {
    Map<String, String> properties = createProperties(dir);
    properties.put("s3-role-arn", "arn:minio:iam:::role/test-role");
    // Without a custom STS endpoint a MinIO role ARN is not a valid target.
    properties.remove("s3-token-service-endpoint");

    try (AwsIrsaCredentialGenerator generator = new AwsIrsaCredentialGenerator()) {
      generator.initialize(properties);

      PathBasedCredentialContext context =
          new PathBasedCredentialContext(
              "test-user", Collections.emptySet(), Collections.singleton("s3://bucket/object"));

      assertThrows(IllegalArgumentException.class, () -> generator.generate(context));
    }
  }

  private Map<String, String> createProperties(Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "configured-token".getBytes(StandardCharsets.UTF_8));

    Map<String, String> properties = new HashMap<>();
    properties.put(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString());
    properties.put("s3-role-arn", "arn:aws:iam::123456789012:role/test-role");
    properties.put("s3-region", "us-east-1");
    properties.put(
        "s3-token-service-endpoint",
        String.format("http://127.0.0.1:%s", server.getAddress().getPort()));
    return properties;
  }

  private void handleAssumeRoleWithWebIdentity(HttpExchange exchange) throws IOException {
    requestCount.incrementAndGet();
    requestBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
    byte[] response =
        ("<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">"
                + "<AssumeRoleWithWebIdentityResult><Credentials>"
                + "<AccessKeyId>access-key</AccessKeyId>"
                + "<SecretAccessKey>secret-key</SecretAccessKey>"
                + "<SessionToken>session-token</SessionToken>"
                + "<Expiration>"
                + Instant.now().plusSeconds(3600)
                + "</Expiration>"
                + "</Credentials></AssumeRoleWithWebIdentityResult>"
                + "</AssumeRoleWithWebIdentityResponse>")
            .getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, response.length);
    exchange.getResponseBody().write(response);
    exchange.close();
  }
}
