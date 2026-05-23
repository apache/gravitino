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
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractIdpMetaStorageTest {
  private static final String H2_BACKEND = "h2";
  private static final String MYSQL_BACKEND = "mysql";
  private static final String POSTGRESQL_BACKEND = "postgresql";
  private static final TestDatabaseName MYSQL_TEST_DATABASE = TestDatabaseName.MYSQL_JDBC_BACKEND;
  private static final TestDatabaseName POSTGRESQL_TEST_DATABASE = TestDatabaseName.PG_JDBC_BACKEND;

  protected JDBCBackend backend;
  public SqlSession sharedSession;

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
    ContainerSuite.getInstance().close();

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
      throw new RuntimeException("Failed to initialize MySQL backend for IdP user tests", e);
    }
  }

  private void initializePostgreSQLBackend(Config backendConfig) throws IOException {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startPostgreSQLContainer(POSTGRESQL_TEST_DATABASE);
    PostgreSQLContainer postgreSQLContainer = containerSuite.getPostgreSQLContainer();
    String schemaName = "idp_user_" + UUID.randomUUID().toString().replace("-", "");
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
