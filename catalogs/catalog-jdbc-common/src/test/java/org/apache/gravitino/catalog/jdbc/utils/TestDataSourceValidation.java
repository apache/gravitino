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
package org.apache.gravitino.catalog.jdbc.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sqlite.SQLiteConnection;

/** Tests default driver validation and explicitly configured SQL validation. */
public class TestDataSourceValidation {

  /** Verifies default validation preserves a live physical SQLite connection across borrows. */
  @Test
  public void testReuseWithDefaultValidation() throws SQLException {
    try (BasicDataSource dataSource = createDataSource(Map.of())) {
      Assertions.assertNull(dataSource.getValidationQuery());
      Assertions.assertTrue(dataSource.getTestOnBorrow());
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE pool_marker (id INTEGER)");
        statement.execute("INSERT INTO pool_marker VALUES (1)");
      }
      // Each physical connection has its own in-memory database, so the marker proves reuse.
      for (int i = 0; i < 3; i++) {
        try (Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery("SELECT id FROM pool_marker")) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals(1, result.getInt(1));
        }
      }
    }
  }

  /** Verifies default validation replaces a closed physical connection. */
  @Test
  public void testClosedConnectionIsReplaced() throws SQLException {
    try (BasicDataSource dataSource = createDataSource(Map.of())) {
      SQLiteConnection oldConnection;
      try (Connection connection = dataSource.getConnection()) {
        oldConnection = connection.unwrap(SQLiteConnection.class);
      }
      oldConnection.close();
      try (Connection connection = dataSource.getConnection()) {
        Assertions.assertNotSame(oldConnection, connection.unwrap(SQLiteConnection.class));
        Assertions.assertTrue(connection.isValid(1));
      }
    }
  }

  /** Verifies a supplied validation query is executed instead of being overwritten. */
  @ParameterizedTest
  @ValueSource(strings = {"SELECT 42", "SELECT * FROM missing_validation_table"})
  public void testExplicitValidationQuery(String query) throws SQLException {
    try (BasicDataSource dataSource = createDataSource(Map.of("validationQuery", query))) {
      Assertions.assertEquals(query, dataSource.getValidationQuery());
      if (query.contains("missing_validation_table")) {
        Assertions.assertThrows(
            SQLException.class,
            () -> {
              try (Connection connection = dataSource.getConnection()) {
                Assertions.assertFalse(connection.isClosed());
              }
            });
      } else {
        try (Connection connection = dataSource.getConnection()) {
          Assertions.assertTrue(connection.isValid(1));
        }
      }
    }
  }

  /** Verifies validation settings remain configurable when using default driver validation. */
  @Test
  public void testValidationSettingsPreserved() throws SQLException {
    try (BasicDataSource dataSource =
        createDataSource(
            Map.of("validationQueryTimeout", "5", JdbcConfig.TEST_ON_BORROW.getKey(), "false"))) {
      Assertions.assertNull(dataSource.getValidationQuery());
      Assertions.assertEquals(
          Duration.ofSeconds(5), dataSource.getValidationQueryTimeoutDuration());
      Assertions.assertFalse(dataSource.getTestOnBorrow());
      try (Connection connection = dataSource.getConnection()) {
        Assertions.assertTrue(connection.isValid(1));
      }
    }
  }

  private static BasicDataSource createDataSource(Map<String, String> overrides) {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MIN_SIZE.getKey(), "1");
    properties.putAll(overrides);
    return (BasicDataSource) DataSourceUtils.createDataSource(properties);
  }
}
