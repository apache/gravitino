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

package org.apache.gravitino.idp.auth;

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.IdpUserGroupManagerTestHelper;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link BasicAuthenticator} backed by an embedded H2 store. */
class TestBasicAuthenticationIntegration {

  private static final String USER = "alice";
  private static final String PASSWORD = "Passw0rd-For-Alice";

  private static Config config;
  private static Path h2Path;
  private static IdpUserGroupManager userGroupManager;
  private static BasicAuthenticator authenticator;

  @BeforeAll
  static void setUp() throws Exception {
    h2Path = Files.createTempDirectory("gravitino_basic_auth_it_");
    config = new Config(false) {};
    config.set(ENTITY_STORE, RELATIONAL_ENTITY_STORE);
    config.set(ENTITY_RELATIONAL_STORE, "h2");
    config.set(
        ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 100);
    config.set(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);
    config.set(STORE_DELETE_AFTER_TIME, 20 * 60 * 1000L);
    config.set(CACHE_ENABLED, false);

    userGroupManager = IdpUserGroupManagerTestHelper.newManager(config, RandomIdGenerator.INSTANCE);
    userGroupManager.addUser(USER, PASSWORD);

    authenticator = new BasicAuthenticator();
    Field userGroupManagerField = BasicAuthenticator.class.getDeclaredField("userGroupManager");
    userGroupManagerField.setAccessible(true);
    userGroupManagerField.set(authenticator, userGroupManager);
  }

  @AfterAll
  static void tearDown() throws IOException {
    authenticator = null;
    if (userGroupManager != null) {
      userGroupManager.close();
      userGroupManager = null;
    }
    if (h2Path != null && Files.exists(h2Path)) {
      try (Stream<Path> paths = Files.walk(h2Path)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(TestBasicAuthenticationIntegration::deletePath);
      }
    }
  }

  @Test
  void testAuthenticateToken() {
    UserPrincipal principal =
        (UserPrincipal)
            authenticator.authenticateToken(basicAuthBytes(basicAuthHeader(USER, PASSWORD)));

    assertEquals(USER, principal.getName());
  }

  @Test
  void testInvalidPassword() {
    UnauthorizedException exception =
        assertThrows(
            UnauthorizedException.class,
            () ->
                authenticator.authenticateToken(
                    basicAuthBytes(basicAuthHeader(USER, "wrong-password"))));

    assertEquals("Invalid username or password", exception.getMessage());
  }

  private static String basicAuthHeader(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] basicAuthBytes(String authHeader) {
    return authHeader.getBytes(StandardCharsets.UTF_8);
  }

  private static void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
