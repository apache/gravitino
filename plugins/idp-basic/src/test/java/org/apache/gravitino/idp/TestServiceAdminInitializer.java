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

package org.apache.gravitino.idp;

import static org.apache.gravitino.Configs.AUTHENTICATORS;
import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.SERVICE_ADMINS;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.auth.BasicAuthenticator;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestServiceAdminInitializer {
  private static final String BASIC_AUTHENTICATOR_CLASS =
      BasicAuthenticator.class.getCanonicalName();
  private static final String VALID_PASSWORD = "Passw0rd-For-Admin1";

  private Path h2Path;
  private Config config;
  private IdpUserGroupManager manager;

  @BeforeEach
  void setUp() throws IOException {
    h2Path = Files.createTempDirectory("gravitino_idp_service_admin_h2_");
    config = createH2Config(h2Path);
    manager = new IdpUserGroupManager(config, RandomIdGenerator.INSTANCE);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    if (h2Path != null && Files.exists(h2Path)) {
      try (Stream<Path> paths = Files.walk(h2Path)) {
        paths.sorted(Comparator.reverseOrder()).forEach(TestServiceAdminInitializer::deletePath);
      }
    }
  }

  @Test
  void testInitializeCreatesMissingServiceAdmin() throws IOException {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "admin1,admin2");
    manager.addUser("admin2", VALID_PASSWORD);

    manager.initializeConfiguredServiceAdmins(config, VALID_PASSWORD);

    assertEquals("admin1", manager.getUser("admin1").name());
    assertEquals("admin2", manager.getUser("admin2").name());
  }

  @Test
  void testInitializeSkipsWhenBasicAuthenticatorDisabledEvenIfPayloadInvalid() throws IOException {
    loadConfig("simple", "admin1");
    manager.initializeConfiguredServiceAdmins(config, "short");
    assertThrows(NotFoundException.class, () -> manager.getUser("admin1"));
  }

  @Test
  void testInitializeSkipsWhenNoServiceAdminsConfigured() throws IOException {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "");
    manager.initializeConfiguredServiceAdmins(config, VALID_PASSWORD);
    assertThrows(NotFoundException.class, () -> manager.getUser("admin1"));
  }

  @Test
  void testInitializeSkipsWhenAllServiceAdminsAlreadyExist() throws IOException {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "admin1,admin2");
    manager.addUser("admin1", VALID_PASSWORD);
    manager.addUser("admin2", VALID_PASSWORD);

    manager.initializeConfiguredServiceAdmins(config, "");

    assertEquals("admin1", manager.getUser("admin1").name());
    assertEquals("admin2", manager.getUser("admin2").name());
  }

  @Test
  void testInitializeFailsWhenRequiredPasswordMissing() {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "admin1");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> manager.initializeConfiguredServiceAdmins(config, ""));

    assertEquals(
        "Missing initial password for configured service admin admin1; declare"
            + " GRAVITINO_INITIAL_ADMIN_PASSWORD",
        exception.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"short"})
  void testInitializeFailsOnInvalidPasswordPayload(String payload) {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "admin1");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> manager.initializeConfiguredServiceAdmins(config, payload));

    assertEquals(
        "Password must be at least 12 characters long and at most 64 characters long",
        exception.getMessage());
  }

  @Test
  void testInitializeUsesSamePasswordForAllMissingServiceAdmins() throws IOException {
    loadConfig(BASIC_AUTHENTICATOR_CLASS, "admin1,admin2");

    manager.initializeConfiguredServiceAdmins(config, VALID_PASSWORD);

    assertEquals("admin1", manager.authenticate("admin1", VALID_PASSWORD).name());
    assertEquals("admin2", manager.authenticate("admin2", VALID_PASSWORD).name());
  }

  private static Config createH2Config(Path h2Path) {
    Config backendConfig = new Config(false) {};
    backendConfig.set(ENTITY_STORE, RELATIONAL_ENTITY_STORE);
    backendConfig.set(ENTITY_RELATIONAL_STORE, "h2");
    backendConfig.set(
        ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 100);
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);
    backendConfig.set(STORE_DELETE_AFTER_TIME, 20 * 60 * 1000L);
    backendConfig.set(CACHE_ENABLED, false);
    return backendConfig;
  }

  private static void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadConfig(String authenticators, String serviceAdmins) {
    config.loadFromMap(
        ImmutableMap.of(
            AUTHENTICATORS.getKey(), authenticators, SERVICE_ADMINS.getKey(), serviceAdmins),
        t -> true);
  }
}
