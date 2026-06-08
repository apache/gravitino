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
package org.apache.gravitino.trino.connector.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

/**
 * End-to-end integration test for Trino connector Basic authentication against the built-in IdP.
 * Exercises the full path: Trino connector plugin → HTTP Basic credentials → Gravitino REST →
 * built-in IdP validation.
 */
@Tag("gravitino-docker-test")
public class TrinoBasicAuthIT {

  private static final Logger LOG = LoggerFactory.getLogger(TrinoBasicAuthIT.class);

  private static final String METALAKE = "trino_basic_auth";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";
  private static final int TRINO_PORT = 8080;

  private static BaseIT baseIT;
  private static GenericContainer<?> trinoContainer;
  private static Path trinoConfigDir;
  private static TrinoQueryRunner trinoQueryRunner;

  @BeforeAll
  static void setUp() throws Exception {
    baseIT = new BaseIT();
    BaseIT.setEnv("GRAVITINO_INITIAL_ADMIN_PASSWORD", ADMIN_PASSWORD);
    baseIT.registerCustomConfigs(idpConfigs());
    baseIT.startIntegrationTest();

    String gravitinoUri = String.format("http://127.0.0.1:%d", baseIT.getGravitinoServerPort());
    try (GravitinoAdminClient adminClient =
        GravitinoAdminClient.builder(gravitinoUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build()) {
      adminClient.createMetalake(METALAKE, "Trino basic auth test metalake", Maps.newHashMap());
    }

    trinoConfigDir =
        prepareTrinoConfigDir(baseIT.getGravitinoServerPort(), METALAKE, ADMIN, ADMIN_PASSWORD);
    trinoContainer = startTrinoContainer(trinoConfigDir, trinoConnectorDir());
    trinoQueryRunner =
        new TrinoQueryRunner(
            String.format("http://127.0.0.1:%d", trinoContainer.getMappedPort(TRINO_PORT)));
  }

  @AfterAll
  static void tearDown() throws IOException, InterruptedException {
    if (trinoQueryRunner != null) {
      trinoQueryRunner.stop();
    }
    if (trinoContainer != null) {
      trinoContainer.stop();
    }
    if (trinoConfigDir != null) {
      try {
        FileUtils.deleteDirectory(trinoConfigDir.toFile());
      } catch (IOException e) {
        LOG.warn("Failed to delete temporary Trino config directory {}", trinoConfigDir, e);
      }
    }
    if (baseIT != null) {
      baseIT.stopIntegrationTest();
    }
  }

  @Test
  void testTrinoConnectorConnectsWithBasicAuth() {
    Assertions.assertDoesNotThrow(() -> trinoQueryRunner.runQuery("SHOW CATALOGS"));
  }

  private static Map<String, String> idpConfigs() {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    configs.put(Configs.REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_EXTENSION_PACKAGE);
    return configs;
  }

  private static GenericContainer<?> startTrinoContainer(Path configDir, String connectorLibDir) {
    Map<String, String> configMounts =
        ImmutableMap.of(
            "config.properties", "/etc/trino/config.properties",
            "node.properties", "/etc/trino/node.properties",
            "jvm.config", "/etc/trino/jvm.config",
            "log4j2.properties", "/etc/trino/log4j2.properties");
    GenericContainer<?> container =
        new GenericContainer<>("trinodb/trino:478")
            .withExposedPorts(TRINO_PORT)
            .withExtraHost("host.docker.internal", "host-gateway");
    configMounts.forEach(
        (file, target) ->
            container.withFileSystemBind(
                configDir.resolve(file).toString(), target, BindMode.READ_ONLY));
    container
        .withFileSystemBind(
            configDir.resolve("catalog").resolve("gravitino.properties").toString(),
            "/etc/trino/catalog/gravitino.properties",
            BindMode.READ_ONLY)
        .withFileSystemBind(connectorLibDir, "/usr/lib/trino/plugin/gravitino", BindMode.READ_ONLY)
        .start();

    String trinoUri = String.format("http://127.0.0.1:%d", container.getMappedPort(TRINO_PORT));
    Awaitility.await()
        .atMost(2, TimeUnit.MINUTES)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                HttpURLConnection connection =
                    (HttpURLConnection) new URL(trinoUri + "/v1/info").openConnection();
                connection.setConnectTimeout(2000);
                connection.setReadTimeout(2000);
                return connection.getResponseCode() == 200;
              } catch (IOException e) {
                return false;
              }
            });
    return container;
  }

  private static Path prepareTrinoConfigDir(
      int gravitinoServerPort, String metalakeName, String username, String password)
      throws IOException {
    Path templateDir =
        Paths.get(
            System.getenv("GRAVITINO_ROOT_DIR"),
            "integration-test-common",
            "docker-script",
            "init",
            "trino",
            "config");
    Path configDir = Files.createTempDirectory("trino-basic-auth-config");
    Files.createDirectories(configDir.resolve("catalog"));

    for (String file : new String[] {"config.properties", "jvm.config", "log4j2.properties"}) {
      Files.copy(templateDir.resolve(file), configDir.resolve(file));
    }
    Files.writeString(
        configDir.resolve("node.properties"),
        Files.readString(templateDir.resolve("node.properties"))
            .replace("NODE_ID", UUID.randomUUID().toString()));
    Files.writeString(
        configDir.resolve("catalog").resolve("gravitino.properties"),
        String.format(
            "connector.name = gravitino%n"
                + "gravitino.uri = http://host.docker.internal:%d%n"
                + "gravitino.metalake = %s%n"
                + "gravitino.trino.skip-version-validation=true%n"
                + "gravitino.client.authType = basic%n"
                + "gravitino.client.basic.username = %s%n"
                + "gravitino.client.basic.password = %s%n",
            gravitinoServerPort, metalakeName, username, password));
    return configDir;
  }

  private static String trinoConnectorDir() {
    Path connectorDir =
        Paths.get(System.getenv("GRAVITINO_ROOT_DIR"))
            .resolve("trino-connector/trino-connector-473-478/build/libs");
    if (!connectorDir.toFile().isDirectory()) {
      throw new IllegalStateException("Trino connector libs not found at " + connectorDir);
    }
    return connectorDir.toString();
  }
}
