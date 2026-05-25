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

package org.apache.gravitino.idp.storage.mapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PGImageName;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractIdpMetaStorageTest {
  private static final String H2_BACKEND = "h2";
  private static final String MYSQL_BACKEND = "mysql";
  private static final String POSTGRESQL_BACKEND = "postgresql";
  private static final String LOCALHOST = "127.0.0.1";
  private static final TestDatabaseName MYSQL_TEST_DATABASE = TestDatabaseName.MYSQL_JDBC_BACKEND;
  private static final TestDatabaseName POSTGRESQL_TEST_DATABASE = TestDatabaseName.PG_JDBC_BACKEND;

  private static MySQLContainer mySQLContainer;
  private static PostgreSQLContainer postgreSQLContainer;
  private static boolean dockerShutdownHookRegistered;

  protected JDBCBackend backend;
  public SqlSession sharedSession;

  protected Config getConfig() {
    return config;
  }

  private Config config;
  private Path h2Path;

  public static Stream<String> storageProvider() {
    return Stream.of(H2_BACKEND, MYSQL_BACKEND, POSTGRESQL_BACKEND);
  }

  @AfterEach
  void closeSuite() throws IOException {
    closeSession();
    if (backend != null) {
      backend.close();
      backend = null;
    }

    SqlSessionFactoryHelper.getInstance().close();

    if (h2Path != null && Files.exists(h2Path)) {
      deleteDirectory(h2Path);
      h2Path = null;
    }
  }

  public void init(String type) throws IOException {
    config = createBackendConfig(type);
    backend = new JDBCBackend();
    backend.initialize(config);
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    initializeMappers();
  }

  protected void closeSession() {
    if (sharedSession != null) {
      sharedSession.close();
      sharedSession = null;
    }
  }

  /**
   * Re-initializes the JDBC backend after another component closed the shared SqlSession factory.
   */
  protected void reinitializeBackend() throws IOException {
    if (backend != null) {
      backend.close();
    }
    backend = new JDBCBackend();
    backend.initialize(config);
  }

  protected void reopenSession() {
    closeSession();
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    initializeMappers();
  }

  protected void initializeMappers() {}

  private Config createBackendConfig(String type) throws IOException {
    Config backendConfig = new Config(false) {};
    backendConfig.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    backendConfig.set(Configs.ENTITY_RELATIONAL_STORE, type);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 20);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);

    switch (type) {
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
        throw new IllegalArgumentException("Unsupported backend type: " + type);
    }

    return backendConfig;
  }

  private void initializeMySQLBackend(Config backendConfig) throws IOException {
    startMySQLContainer();
    String jdbcUrl = getMySQLJdbcUrl(MYSQL_TEST_DATABASE);

    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, jdbcUrl);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, MySQLContainer.USER_NAME);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, MySQLContainer.PASSWORD);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "com.mysql.cj.jdbc.Driver");

    try (Connection connection =
            openConnectionWithRetry(
                getMySQLServerJdbcUrl(), MySQLContainer.USER_NAME, MySQLContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS " + MYSQL_TEST_DATABASE);
      statement.execute("CREATE DATABASE " + MYSQL_TEST_DATABASE);
      statement.execute("USE " + MYSQL_TEST_DATABASE);
      executeSqlStatements(statement, loadSchemaStatements(MYSQL_BACKEND));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize MySQL backend for IdP user tests", e);
    }
  }

  private void initializePostgreSQLBackend(Config backendConfig) throws IOException {
    startPostgreSQLContainer();
    createPostgreSQLDatabase(POSTGRESQL_TEST_DATABASE);
    String schemaName = "idp_user_" + UUID.randomUUID().toString().replace("-", "");
    String jdbcUrl =
        getPostgreSQLJdbcUrl(POSTGRESQL_TEST_DATABASE) + "&currentSchema=" + schemaName;

    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, jdbcUrl);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, PostgreSQLContainer.USER_NAME);
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, PostgreSQLContainer.PASSWORD);
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.postgresql.Driver");

    try (Connection connection =
            openConnectionWithRetry(
                getPostgreSQLJdbcUrl(POSTGRESQL_TEST_DATABASE),
                PostgreSQLContainer.USER_NAME,
                PostgreSQLContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
      statement.execute("CREATE SCHEMA " + schemaName);
      statement.execute("SET search_path TO " + schemaName);
      executeSqlStatements(statement, loadSchemaStatements(POSTGRESQL_BACKEND));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize PostgreSQL backend for IdP user tests", e);
    }
  }

  private void initializeH2Backend(Config backendConfig) throws IOException {
    h2Path = Files.createTempDirectory("gravitino_idp_basic_h2_");
    backendConfig.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "root");
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "123456");
    backendConfig.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
  }

  private static synchronized void startMySQLContainer() {
    ITUtils.cleanDisk();
    if (mySQLContainer == null) {
      mySQLContainer =
          MySQLContainer.builder()
              .withHostName("gravitino-idp-mysql")
              .withEnvVars(ImmutableMap.of("MYSQL_ROOT_PASSWORD", "root"))
              .withExposePorts(ImmutableSet.of(MySQLContainer.MYSQL_PORT))
              .build();
      mySQLContainer.start();
      registerDockerShutdownHook();
    }
  }

  private static synchronized void startPostgreSQLContainer() {
    ITUtils.cleanDisk();
    if (postgreSQLContainer == null) {
      postgreSQLContainer =
          PostgreSQLContainer.builder()
              .withImage(PGImageName.VERSION_13.toString())
              .withHostName("gravitino-idp-pg")
              .withEnvVars(
                  ImmutableMap.of(
                      "POSTGRES_USER", PostgreSQLContainer.USER_NAME,
                      "POSTGRES_PASSWORD", PostgreSQLContainer.PASSWORD))
              .withExposePorts(ImmutableSet.of(PostgreSQLContainer.PG_PORT))
              .build();
      postgreSQLContainer.start();
      registerDockerShutdownHook();
    }
  }

  private static String getMySQLJdbcUrl(TestDatabaseName databaseName) {
    startMySQLContainer();
    return String.format(
        "jdbc:mysql://%s:%d/%s",
        LOCALHOST, mySQLContainer.getMappedPort(MySQLContainer.MYSQL_PORT), databaseName);
  }

  private static String getMySQLServerJdbcUrl() {
    startMySQLContainer();
    return String.format(
        "jdbc:mysql://%s:%d/", LOCALHOST, mySQLContainer.getMappedPort(MySQLContainer.MYSQL_PORT));
  }

  private static String getPostgreSQLJdbcUrl(TestDatabaseName databaseName) {
    startPostgreSQLContainer();
    return String.format(
        "jdbc:postgresql://%s:%d/%s?sslmode=disable",
        LOCALHOST, postgreSQLContainer.getMappedPort(PostgreSQLContainer.PG_PORT), databaseName);
  }

  private static String getPostgreSQLServerJdbcUrl() {
    startPostgreSQLContainer();
    return String.format(
        "jdbc:postgresql://%s:%d/?sslmode=disable",
        LOCALHOST, postgreSQLContainer.getMappedPort(PostgreSQLContainer.PG_PORT));
  }

  private static void createPostgreSQLDatabase(TestDatabaseName databaseName) {
    try (Connection connection =
            openConnectionWithRetry(
                getPostgreSQLServerJdbcUrl(),
                PostgreSQLContainer.USER_NAME,
                PostgreSQLContainer.PASSWORD);
        Statement statement = connection.createStatement()) {
      statement.execute(String.format("CREATE DATABASE \"%s\"", databaseName));
    } catch (SQLException e) {
      if (e.getMessage() != null
          && e.getMessage()
              .contains(String.format("database \"%s\" already exists", databaseName))) {
        return;
      }
      throw new RuntimeException("Failed to create PostgreSQL database: " + databaseName, e);
    }
  }

  private static Connection openConnectionWithRetry(String jdbcUrl, String user, String password)
      throws SQLException {
    try {
      return Awaitility.await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .ignoreExceptions()
          .until(
              () -> {
                try {
                  return DriverManager.getConnection(jdbcUrl, user, password);
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              },
              connection -> connection != null);
    } catch (RuntimeException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw new SQLException("Failed to open JDBC connection to " + jdbcUrl, e);
    }
  }

  private static synchronized void registerDockerShutdownHook() {
    if (dockerShutdownHookRegistered) {
      return;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (mySQLContainer != null) {
                    mySQLContainer.close();
                    mySQLContainer = null;
                  }
                  if (postgreSQLContainer != null) {
                    postgreSQLContainer.close();
                    postgreSQLContainer = null;
                  }
                },
                "idp-basic-docker-shutdown"));
    dockerShutdownHookRegistered = true;
  }

  private String[] loadSchemaStatements(String databaseType) throws IOException {
    Path scriptPath =
        resolveProjectRoot()
            .resolve("scripts")
            .resolve(databaseType)
            .resolve(
                String.format(
                    "schema-%s-%s.sql", ConfigConstants.CURRENT_SCRIPT_VERSION, databaseType));
    return Arrays.stream(Files.readString(scriptPath).split(";"))
        .map(String::trim)
        .filter(sql -> !sql.isEmpty())
        .toArray(String[]::new);
  }

  private Path resolveProjectRoot() {
    String rootDir = System.getenv("GRAVITINO_ROOT_DIR");
    if (StringUtils.isBlank(rootDir)) {
      rootDir = System.getenv("GRAVITINO_HOME");
    }

    if (StringUtils.isBlank(rootDir)) {
      throw new IllegalStateException(
          "GRAVITINO_ROOT_DIR or GRAVITINO_HOME must be set for IdP user storage tests");
    }

    return Path.of(rootDir);
  }

  private void executeSqlStatements(Statement statement, String[] sqlStatements)
      throws SQLException {
    for (String sql : sqlStatements) {
      statement.execute(sql);
    }
  }

  private void deleteDirectory(Path directory) throws IOException {
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.sorted(Comparator.reverseOrder()).forEach(this::deletePath);
    }
  }

  private void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException("Delete path failed: " + path, e);
    }
  }
}
