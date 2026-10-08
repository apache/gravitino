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
package org.apache.gravitino.catalog.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

/** Exercises default validation against a real PostgreSQL server. */
@Tag("gravitino-docker-test")
public class TestPostgreSqlDataSource {
  private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:13");

  /** Starts PostgreSQL and creates a schema for connection state changes. */
  @BeforeAll
  public static void startPostgres() throws SQLException {
    POSTGRES.start();
    try (Connection connection = observerConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA pool_target");
    }
  }

  /** Stops the test server. */
  @AfterAll
  public static void stopPostgres() {
    POSTGRES.stop();
  }

  /** Verifies schema changes do not prevent reuse with default driver validation. */
  @Test
  public void testReuseAfterSwitchingSchema() throws SQLException {
    try (BasicDataSource dataSource = createDataSource()) {
      Assertions.assertNull(dataSource.getValidationQuery());
      long expectedId;
      try (Connection connection = dataSource.getConnection()) {
        connection.setSchema("pool_target");
        expectedId = connectionId(connection);
      }
      for (int i = 0; i < 100; i++) {
        try (Connection connection = dataSource.getConnection()) {
          connection.setSchema("pool_target");
          Assertions.assertEquals(expectedId, connectionId(connection));
          try (Statement statement = connection.createStatement();
              ResultSet result = statement.executeQuery("SELECT current_schema()")) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals("pool_target", result.getString(1));
          }
        }
      }
    }
  }

  /** Verifies borrow validation replaces a terminated PostgreSQL connection. */
  @Test
  public void testDeadConnectionIsReplaced() throws SQLException {
    try (BasicDataSource dataSource = createDataSource()) {
      long oldId;
      try (Connection connection = dataSource.getConnection()) {
        oldId = connectionId(connection);
      }
      try (Connection observer = observerConnection();
          Statement statement = observer.createStatement();
          ResultSet result = statement.executeQuery("SELECT pg_terminate_backend(" + oldId + ")")) {
        Assertions.assertTrue(result.next());
        Assertions.assertTrue(result.getBoolean(1));
      }
      try (Connection connection = dataSource.getConnection()) {
        Assertions.assertNotEquals(oldId, connectionId(connection));
      }
    }
  }

  private static BasicDataSource createDataSource() {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_URL.getKey(), POSTGRES.getJdbcUrl());
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), POSTGRES.getDriverClassName());
    properties.put(JdbcConfig.USERNAME.getKey(), POSTGRES.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), POSTGRES.getPassword());
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MIN_SIZE.getKey(), "1");
    return (BasicDataSource) DataSourceUtils.createDataSource(properties);
  }

  private static Connection observerConnection() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static long connectionId(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
      Assertions.assertTrue(result.next());
      return result.getLong(1);
    }
  }
}
