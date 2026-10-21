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
package org.apache.gravitino.catalog.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.integration.test.container.FixtureContainer;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.MySQLFixture;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Exercises catalog switching and connection validation against a real MySQL server. */
@Tag("gravitino-docker-test")
@MySQLFixture(database = TestDatabaseName.MYSQL_TEST_MYSQL_DATA_SOURCE)
public class TestMysqlDataSource {
  private static final TestDatabaseName DEFAULT_DATABASE =
      TestDatabaseName.MYSQL_TEST_MYSQL_DATA_SOURCE;
  private static final String TARGET_DATABASE = "test_mysql_data_source_target";

  @FixtureContainer private static MySQLContainer mysql;

  /** Creates a target database distinct from the default database. */
  @BeforeAll
  public static void createTargetDatabase() {
    mysql.execute(List.of("CREATE DATABASE IF NOT EXISTS " + TARGET_DATABASE));
  }

  /** Verifies repeated catalog switches reuse the same physical connection. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReuseAfterSwitchingCatalog(boolean defaultDatabase) throws SQLException {
    try (BasicDataSource dataSource = createDataSource(defaultDatabase)) {
      long expectedId;
      try (Connection connection = dataSource.getConnection()) {
        // Guard the URL rewrite: without a default database, the case must start with none.
        Assertions.assertEquals(
            defaultDatabase ? DEFAULT_DATABASE.toString() : null, currentDatabase(connection));
        connection.setCatalog(TARGET_DATABASE);
        expectedId = connectionId(connection);
      }
      // An idle count of one also occurs when validation destroys and replaces the connection.
      // Check the physical server connection ID on every borrow instead.
      for (int i = 0; i < 100; i++) {
        try (Connection connection = dataSource.getConnection()) {
          connection.setCatalog(TARGET_DATABASE);
          Assertions.assertEquals(expectedId, connectionId(connection));
          Assertions.assertEquals(TARGET_DATABASE, currentDatabase(connection));
        }
      }
    }
  }

  /** Verifies borrow validation rejects and replaces a disconnected connection. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testDeadConnectionIsReplaced(boolean defaultDatabase) throws SQLException {
    try (BasicDataSource dataSource = createDataSource(defaultDatabase)) {
      long oldId;
      try (Connection connection = dataSource.getConnection()) {
        connection.setCatalog(TARGET_DATABASE);
        oldId = connectionId(connection);
      }
      mysql.execute(List.of("KILL CONNECTION " + oldId));
      try (Connection connection = dataSource.getConnection()) {
        connection.setCatalog(TARGET_DATABASE);
        Assertions.assertNotEquals(oldId, connectionId(connection));
      }
    }
  }

  private static BasicDataSource createDataSource(boolean defaultDatabase) {
    Map<String, String> properties = new HashMap<>();
    String url = defaultDatabase ? mysql.getJdbcUrl(DEFAULT_DATABASE) : mysql.getJdbcUrl() + "/";
    properties.put(JdbcConfig.JDBC_URL.getKey(), url);
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.USERNAME.getKey(), mysql.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), mysql.getPassword());
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MIN_SIZE.getKey(), "1");
    return (BasicDataSource) DataSourceUtils.createDataSource(properties);
  }

  @Nullable
  private static String currentDatabase(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT DATABASE()")) {
      Assertions.assertTrue(result.next());
      return result.getString(1);
    }
  }

  private static long connectionId(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT CONNECTION_ID()")) {
      Assertions.assertTrue(result.next());
      return result.getLong(1);
    }
  }
}
