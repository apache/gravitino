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
package org.apache.gravitino.catalog.glue.integration.test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.glue.GlueCatalogOperations;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.integration.test.container.GravitinoMotoContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Runs {@link AbstractGlueCatalogIT} scenarios against a Moto server (free AWS mock).
 *
 * <p>Requires Docker. Skipped by default when the {@code gravitino-docker-test} tag is excluded.
 * Override the container image with the {@code GRAVITINO_CI_MOTO_DOCKER_IMAGE} environment
 * variable.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MotoGlueCatalogIT extends AbstractGlueCatalogIT {

  private static final String FALLBACK_IMAGE = "motoserver/moto:5.1.4";

  private GravitinoMotoContainer moto;

  @Override
  @BeforeAll
  void initOps() {
    String image = GravitinoMotoContainer.DEFAULT_IMAGE;
    if (image == null || image.isBlank()) {
      image = FALLBACK_IMAGE;
    }
    moto = GravitinoMotoContainer.builder().withImage(image).build();
    moto.start();
    super.initOps();
  }

  @AfterAll
  void stopContainer() {
    if (moto != null) {
      moto.close();
    }
  }

  @Test
  void testUnreachableEndpointIsConnectionFailure() throws IOException {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    String endpoint = "http://127.0.0.1:" + closedPort;

    ConnectionFailedException e =
        assertThrows(ConnectionFailedException.class, () -> listSchemasAt(endpoint, Map.of()));
    assertTrue(e.getMessage().contains(endpoint), e.getMessage());
  }

  @Test
  void testEndpointThatDoesNotAnswerTimesOut() throws IOException {
    // The socket accepts connections but never answers, like a stalled endpoint.
    try (ServerSocket silent = new ServerSocket(0)) {
      String endpoint = "http://127.0.0.1:" + silent.getLocalPort();
      Map<String, String> timeouts =
          Map.of(
              GlueConstants.AWS_GLUE_API_CALL_TIMEOUT_MS, "3000",
              GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS, "1000",
              GlueConstants.AWS_GLUE_MAX_ATTEMPTS, "2");

      long start = System.nanoTime();
      ConnectionFailedException e =
          assertThrows(ConnectionFailedException.class, () -> listSchemasAt(endpoint, timeouts));
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      assertTrue(e.getMessage().contains("timed out"), e.getMessage());
      assertTrue(elapsedMs < 10_000, "took " + elapsedMs + " ms");
    }
  }

  private void listSchemasAt(String endpoint, Map<String, String> extraConfig) throws IOException {
    Map<String, String> config = new HashMap<>(catalogConfig());
    config.put(GlueConstants.AWS_GLUE_ENDPOINT, endpoint);
    config.putAll(extraConfig);
    GlueCatalogOperations unreachableOps = new GlueCatalogOperations();
    try {
      unreachableOps.initialize(config, null, null);
      unreachableOps.listSchemas(Namespace.of("ml", "cat"));
    } finally {
      unreachableOps.close();
    }
  }

  @Override
  protected Map<String, String> catalogConfig() {
    String endpoint =
        "http://"
            + moto.getContainer().getHost()
            + ":"
            + moto.getMappedPort(GravitinoMotoContainer.PORT);
    return Map.of(
        GlueConstants.AWS_REGION,
        "us-east-1",
        GlueConstants.AWS_ACCESS_KEY_ID,
        "test",
        GlueConstants.AWS_SECRET_ACCESS_KEY,
        "test",
        GlueConstants.AWS_GLUE_ENDPOINT,
        endpoint,
        GlueConstants.WAREHOUSE,
        "s3://gravitino-moto-test/warehouse");
  }
}
