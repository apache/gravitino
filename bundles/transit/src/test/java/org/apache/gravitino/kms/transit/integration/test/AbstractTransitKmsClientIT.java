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
package org.apache.gravitino.kms.transit.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsClientFactory;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

abstract class AbstractTransitKmsClientIT {

  private static final int API_PORT = 8200;
  private static final String ROOT_TOKEN = "gravitino-kms-test-root-token";
  private static final String SOURCE = "test";
  private static final String USABLE_KEY = "usable-key";
  private static final String SIGNING_KEY = "signing-key";

  private GenericContainer<?> container;
  private Path tokenFile;

  @BeforeAll
  void startBackend() throws Exception {
    tokenFile = Files.createTempFile("gravitino-transit-token-", ".txt");
    Files.write(tokenFile, ROOT_TOKEN.getBytes(StandardCharsets.UTF_8));

    container =
        new GenericContainer<>(DockerImageName.parse(image()))
            .withCommand(
                "server",
                "-dev",
                String.format("-dev-root-token-id=%s", ROOT_TOKEN),
                "-dev-listen-address=0.0.0.0:8200")
            .withEnv(addressEnvironmentVariable(), "http://127.0.0.1:8200")
            .withEnv(tokenEnvironmentVariable(), ROOT_TOKEN)
            .withExposedPorts(API_PORT)
            .waitingFor(
                Wait.forHttp("/v1/sys/health")
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    container.start();

    runCommand("secrets", "enable", "transit");
    runCommand("write", "-f", String.format("transit/keys/%s", USABLE_KEY));
    runCommand("write", "-f", String.format("transit/keys/%s", SIGNING_KEY), "type=ecdsa-p256");
  }

  @AfterAll
  void stopBackend() throws Exception {
    if (container != null) {
      container.stop();
    }
    if (tokenFile != null) {
      Files.deleteIfExists(tokenFile);
    }
  }

  @Test
  void inspectsRealTransitKeyMetadata() {
    try (KmsClient client = factory().create(SOURCE, properties())) {
      KmsReference usableReference = new KmsReference(api(), SOURCE, USABLE_KEY);
      KmsKeyProperties usable = client.getKeyProperties(usableReference);

      assertEquals(usableReference, usable.reference());
      assertTrue(usable.present());
      assertTrue(usable.enabled());
      assertTrue(usable.supportsWrapping());
      assertTrue(usable.supportsUnwrapping());

      KmsKeyProperties signing =
          client.getKeyProperties(new KmsReference(api(), SOURCE, SIGNING_KEY));
      assertTrue(signing.present());
      assertTrue(signing.enabled());
      assertFalse(signing.supportsWrapping());
      assertFalse(signing.supportsUnwrapping());

      KmsKeyProperties missing =
          client.getKeyProperties(new KmsReference(api(), SOURCE, "missing-key"));
      assertFalse(missing.present());
      assertFalse(missing.enabled());
      assertFalse(missing.supportsWrapping());
      assertFalse(missing.supportsUnwrapping());
    }
  }

  protected abstract String image();

  protected abstract String executable();

  protected abstract String addressEnvironmentVariable();

  protected abstract String tokenEnvironmentVariable();

  protected abstract KmsApi api();

  protected abstract KmsClientFactory factory();

  private Map<String, String> properties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        "endpoint.address",
        String.format("http://%s:%s", container.getHost(), container.getMappedPort(API_PORT)));
    properties.put("credential.method", "token_file");
    properties.put("credential.path", tokenFile.toString());
    return properties;
  }

  private void runCommand(String... arguments) throws Exception {
    String[] command = new String[arguments.length + 1];
    command[0] = executable();
    System.arraycopy(arguments, 0, command, 1, arguments.length);
    Container.ExecResult result = container.execInContainer(command);
    assertEquals(
        0,
        result.getExitCode(),
        () ->
            String.format(
                "Transit fixture command failed: stdout=%s, stderr=%s",
                result.getStdout(), result.getStderr()));
  }
}
