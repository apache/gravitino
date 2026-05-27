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
package org.apache.gravitino.idp.integration.test;

import static org.apache.gravitino.integration.test.util.BaseIT.setEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.apache.gravitino.idp.dto.requests.AddGroupRequest;
import org.apache.gravitino.idp.dto.requests.AddUserRequest;
import org.apache.gravitino.idp.dto.requests.ChangePasswordRequest;
import org.apache.gravitino.idp.dto.requests.GroupMembershipChangeRequest;
import org.apache.gravitino.idp.dto.responses.IdpGroupResponse;
import org.apache.gravitino.idp.dto.responses.IdpUserResponse;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * End-to-end tests for built-in IdP REST APIs on an embedded Gravitino server.
 *
 * <p>Runs the same REST scenario against H2, MySQL, and PostgreSQL relational backends.
 */
@Tag("gravitino-docker-test")
public class IdpRestApiIT {

  private static final String H2_BACKEND = "h2";
  private static final String MYSQL_BACKEND = "mysql";
  private static final String POSTGRESQL_BACKEND = "postgresql";

  private static final TestDatabaseName MYSQL_TEST_DATABASE = TestDatabaseName.MYSQL_JDBC_BACKEND;
  private static final TestDatabaseName POSTGRESQL_TEST_DATABASE = TestDatabaseName.PG_JDBC_BACKEND;

  private static final String ACCEPT = "application/vnd.gravitino.v1+json";
  private static final String IDP_REST_EXTENSION_PACKAGE =
      "org.apache.gravitino.idp.web.rest.feature";
  private static final String INITIAL_ADMIN_PASSWORD_ENV = "GRAVITINO_INITIAL_ADMIN_PASSWORD";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";
  private static final String USER1 = "user1";
  private static final String USER2 = "user2";
  private static final String GROUP1 = "group1";
  private static final String USER_PASSWORD = "Passw0rd-For-User1";
  private static final String UPDATED_PASSWORD = "Passw0rd-For-User2";

  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private Path h2Path;

  static Stream<String> jdbcBackends() {
    return Stream.of(H2_BACKEND, MYSQL_BACKEND, POSTGRESQL_BACKEND);
  }

  @ParameterizedTest
  @MethodSource("jdbcBackends")
  public void testIdpRestApis(String backendType) throws Exception {
    setEnv(INITIAL_ADMIN_PASSWORD_ENV, ADMIN_PASSWORD);
    Config relationalConfig = createRelationalBackendConfig(backendType);
    int httpPort = RESTUtils.findAvailablePort(5000, 6000);
    ServerConfig serverConfig = newServerConfig(httpPort, relationalConfig);

    GravitinoServer gravitinoServer = new GravitinoServer(serverConfig, GravitinoEnv.getInstance());
    IdpUserGroupManager idpUserGroupManager = null;
    try {
      gravitinoServer.initialize();
      gravitinoServer.start();
      String apiBase = String.format("http://localhost:%d/api", httpPort);
      idpUserGroupManager =
          new IdpUserGroupManager(serverConfig, GravitinoEnv.getInstance().idGenerator());

      assertEquals(200, get(apiBase, "/version", ADMIN, ADMIN_PASSWORD).statusCode());
      // No Authorization: simple authenticator allows anonymous access; IdP filter rejects.
      assertEquals(403, get(apiBase, "/idp/users/" + USER1, null, null).statusCode());

      postUser(apiBase, USER2, USER_PASSWORD);
      assertEquals(403, get(apiBase, "/idp/users/" + USER2, USER2, USER_PASSWORD).statusCode());
      deleteUser(apiBase, USER2);

      postUser(apiBase, USER1, USER_PASSWORD);
      IdpUserResponse user = getUser(apiBase, USER1);
      assertEquals(USER1, user.getUser().name());
      assertTrue(user.getUser().groups().isEmpty());

      changePassword(apiBase, USER1, UPDATED_PASSWORD);
      assertEquals(USER1, getUser(apiBase, USER1).getUser().name());

      assertTrue(deleteUser(apiBase, USER1));
      assertEquals(
          ErrorConstants.NOT_FOUND_CODE,
          errorCode(get(apiBase, "/idp/users/" + USER1, ADMIN, ADMIN_PASSWORD)));

      postUser(apiBase, USER1, USER_PASSWORD);
      postUser(apiBase, USER2, USER_PASSWORD);

      IdpGroupResponse group = postGroup(apiBase, GROUP1);
      assertEquals(GROUP1, group.getGroup().name());
      assertTrue(group.getGroup().users().isEmpty());

      group =
          putMembership(
              apiBase, GROUP1, new GroupMembershipChangeRequest(new String[] {USER1, USER2}, null));
      assertEquals(List.of(USER1, USER2), group.getGroup().users());

      group =
          putMembership(
              apiBase, GROUP1, new GroupMembershipChangeRequest(null, new String[] {USER1, USER2}));
      assertTrue(group.getGroup().users().isEmpty());

      assertTrue(deleteGroup(apiBase, GROUP1, false));

      deleteUser(apiBase, USER1);
      deleteUser(apiBase, USER2);
    } finally {
      if (gravitinoServer != null) {
        gravitinoServer.stop();
      }
      if (idpUserGroupManager != null) {
        idpUserGroupManager.close();
      }
      cleanupRelationalBackend(backendType);
    }
  }

  private Config createRelationalBackendConfig(String backendType) throws IOException {
    Config backendConfig = new Config(false) {};
    backendConfig.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    backendConfig.set(Configs.ENTITY_RELATIONAL_STORE, backendType);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 100);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

    switch (backendType) {
      case MYSQL_BACKEND:
        initializeMySQLBackend(backendConfig);
        break;
      case POSTGRESQL_BACKEND:
        initializePostgreSQLBackend(backendConfig);
        break;
      case H2_BACKEND:
        initializeH2Backend(backendConfig);
        break;
      default:
        throw new IllegalArgumentException("Unsupported backend type: " + backendType);
    }
    return backendConfig;
  }

  private void cleanupRelationalBackend(String backendType) throws IOException {
    if (H2_BACKEND.equals(backendType) && h2Path != null && Files.exists(h2Path)) {
      try (Stream<Path> paths = Files.walk(h2Path)) {
        paths.sorted(Comparator.reverseOrder()).forEach(IdpRestApiIT::deletePath);
      }
      h2Path = null;
    }
    if (MYSQL_BACKEND.equals(backendType) || POSTGRESQL_BACKEND.equals(backendType)) {
      ContainerSuite.getInstance().close();
    }
  }

  private void initializeMySQLBackend(Config backendConfig) throws IOException {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(MYSQL_TEST_DATABASE);
    MySQLContainer mySQLContainer = containerSuite.getMySQLContainer();
    String jdbcUrl = mySQLContainer.getJdbcUrl(MYSQL_TEST_DATABASE);

    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, jdbcUrl);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, mySQLContainer.getUsername());
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, mySQLContainer.getPassword());
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "com.mysql.cj.jdbc.Driver");

    try (Connection connection =
            DriverManager.getConnection(
                StringUtils.substringBeforeLast(jdbcUrl, "/"),
                mySQLContainer.getUsername(),
                mySQLContainer.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + MYSQL_TEST_DATABASE);
      statement.execute("CREATE DATABASE " + MYSQL_TEST_DATABASE);
      statement.execute("USE " + MYSQL_TEST_DATABASE);
      executeSqlStatements(statement, loadSchemaStatements(MYSQL_BACKEND));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize MySQL backend for IdP REST API IT", e);
    }
  }

  private void initializePostgreSQLBackend(Config backendConfig) throws IOException {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startPostgreSQLContainer(POSTGRESQL_TEST_DATABASE);
    PostgreSQLContainer postgreSQLContainer = containerSuite.getPostgreSQLContainer();
    String schemaName = "idp_rest_" + UUID.randomUUID().toString().replace("-", "");
    String jdbcUrl = postgreSQLContainer.getJdbcUrl(POSTGRESQL_TEST_DATABASE);

    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, jdbcUrl + "?currentSchema=" + schemaName);
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, postgreSQLContainer.getUsername());
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, postgreSQLContainer.getPassword());
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.postgresql.Driver");

    try (Connection connection =
            DriverManager.getConnection(
                jdbcUrl, postgreSQLContainer.getUsername(), postgreSQLContainer.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
      statement.execute("CREATE SCHEMA " + schemaName);
      statement.execute("SET search_path TO " + schemaName);
      executeSqlStatements(statement, loadSchemaStatements(POSTGRESQL_BACKEND));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize PostgreSQL backend for IdP REST API IT", e);
    }
  }

  private void initializeH2Backend(Config backendConfig) throws IOException {
    h2Path = Files.createTempDirectory("gravitino_idp_rest_api_it_");
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
  }

  private static String[] loadSchemaStatements(String databaseType) throws IOException {
    Path scriptPath =
        resolveProjectRoot()
            .resolve("scripts")
            .resolve(databaseType)
            .resolve(
                String.format(
                    "schema-%s-%s.sql", ConfigConstants.CURRENT_SCRIPT_VERSION, databaseType));
    return Arrays.stream(Files.readString(scriptPath).split(";"))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .toArray(String[]::new);
  }

  private static Path resolveProjectRoot() {
    String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
    if (StringUtils.isBlank(rootDir)) {
      rootDir = System.getenv("GRAVITINO_HOME");
    }
    if (StringUtils.isBlank(rootDir)) {
      throw new IllegalStateException(
          "GRAVITINO_ROOT_DIR or GRAVITINO_HOME must be set for IdP REST API IT");
    }
    return Path.of(rootDir);
  }

  private static void executeSqlStatements(Statement statement, String[] sqlStatements)
      throws SQLException {
    for (String sql : sqlStatements) {
      statement.execute(sql);
    }
  }

  private static void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Delete path failed: " + path, e);
    }
  }

  private static ServerConfig newServerConfig(int httpPort, Config relationalConfig) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put(
                GravitinoServer.WEBSERVER_CONF_PREFIX
                    + JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
                String.valueOf(httpPort))
            .put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE)
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
                relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL))
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER.getKey(),
                relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER.getKey(),
                relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER))
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD.getKey(),
                relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD))
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS.getKey(),
                String.valueOf(
                    relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)))
            .put(
                Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS.getKey(),
                String.valueOf(
                    relationalConfig.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)))
            .put(Configs.STORE_DELETE_AFTER_TIME.getKey(), String.valueOf(20 * 60 * 1000L))
            .put(Configs.CACHE_ENABLED.getKey(), "false")
            .put(Configs.ENABLE_AUTHORIZATION.getKey(), "false")
            .put(Configs.SERVICE_ADMINS.getKey(), ADMIN)
            .put(Configs.REST_API_EXTENSION_PACKAGES.getKey(), IDP_REST_EXTENSION_PACKAGE);

    ServerConfig config = new ServerConfig();
    config.loadFromMap(builder.build(), key -> true);
    ServerConfig spyConfig = Mockito.spy(config);
    Mockito.when(
            spyConfig.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX))
        .thenReturn(ImmutableMap.of(AuxiliaryServiceManager.AUX_SERVICE_NAMES, ""));
    return spyConfig;
  }

  private static HttpResponse<String> get(
      String apiBase, String path, String username, String password) throws Exception {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder().uri(URI.create(apiBase + path)).header("Accept", ACCEPT).GET();
    if (StringUtils.isNotBlank(username)) {
      builder.header("Authorization", basicAuth(username, password));
    }
    return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static void postUser(String apiBase, String username, String password) throws Exception {
    HttpResponse<String> response =
        post(apiBase, "/idp/users", new AddUserRequest(username, password));
    assertEquals(200, response.statusCode(), response.body());
    JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class).validate();
  }

  private static IdpUserResponse getUser(String apiBase, String username) throws Exception {
    HttpResponse<String> response = get(apiBase, "/idp/users/" + username, ADMIN, ADMIN_PASSWORD);
    assertEquals(200, response.statusCode(), response.body());
    IdpUserResponse userResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpUserResponse.class);
    userResponse.validate();
    return userResponse;
  }

  private static void changePassword(String apiBase, String username, String password)
      throws Exception {
    HttpResponse<String> response =
        put(apiBase, "/idp/users/" + username, new ChangePasswordRequest(password));
    assertEquals(200, response.statusCode(), response.body());
  }

  private static boolean deleteUser(String apiBase, String username) throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            authorized(ADMIN, ADMIN_PASSWORD)
                .uri(URI.create(apiBase + "/idp/users/" + username))
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private static IdpGroupResponse postGroup(String apiBase, String groupName) throws Exception {
    HttpResponse<String> response = post(apiBase, "/idp/groups", new AddGroupRequest(groupName));
    assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private static IdpGroupResponse putMembership(
      String apiBase, String groupName, GroupMembershipChangeRequest request) throws Exception {
    HttpResponse<String> response = put(apiBase, "/idp/groups/" + groupName + "/users", request);
    assertEquals(200, response.statusCode(), response.body());
    IdpGroupResponse groupResponse =
        JsonUtils.objectMapper().readValue(response.body(), IdpGroupResponse.class);
    groupResponse.validate();
    return groupResponse;
  }

  private static boolean deleteGroup(String apiBase, String groupName, boolean force)
      throws Exception {
    HttpResponse<String> response =
        HTTP.send(
            authorized(ADMIN, ADMIN_PASSWORD)
                .uri(URI.create(apiBase + "/idp/groups/" + groupName + "?force=" + force))
                .DELETE()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return JsonUtils.objectMapper().readTree(response.body()).get("removed").asBoolean();
  }

  private static HttpResponse<String> post(String apiBase, String path, Object body)
      throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + path))
            .header("Content-Type", ACCEPT)
            .POST(jsonBody(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpResponse<String> put(String apiBase, String path, Object body)
      throws Exception {
    return HTTP.send(
        authorized(ADMIN, ADMIN_PASSWORD)
            .uri(URI.create(apiBase + path))
            .header("Content-Type", ACCEPT)
            .PUT(jsonBody(body))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private static HttpRequest.BodyPublisher jsonBody(Object body) throws Exception {
    return HttpRequest.BodyPublishers.ofString(JsonUtils.objectMapper().writeValueAsString(body));
  }

  private static HttpRequest.Builder authorized(String username, String password) {
    return HttpRequest.newBuilder()
        .header("Accept", ACCEPT)
        .header("Authorization", basicAuth(username, password));
  }

  private static String basicAuth(String username, String password) {
    return AuthConstants.AUTHORIZATION_BASIC_HEADER
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private static int errorCode(HttpResponse<String> response) throws Exception {
    return JsonUtils.objectMapper().readTree(response.body()).get("code").asInt();
  }
}
