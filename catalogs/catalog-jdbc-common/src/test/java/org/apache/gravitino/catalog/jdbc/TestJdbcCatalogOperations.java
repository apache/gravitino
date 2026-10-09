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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.SqliteDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJdbcCatalogOperations {

  @Test
  public void testExistingCatalogConnectionFailure() {
    SQLException cause = new SQLException("connection refused");
    SqliteDatabaseOperations databaseOperations =
        new SqliteDatabaseOperations("/unused") {
          @Override
          public List<String> listDatabases() {
            throw new GravitinoRuntimeException(cause, cause.getMessage());
          }
        };

    try (JdbcCatalogOperations catalogOperations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            databaseOperations,
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter())) {
      ConnectionFailedException exception =
          Assertions.assertThrows(
              ConnectionFailedException.class,
              () -> catalogOperations.testConnection(NameIdentifier.of("metalake", "catalog")));
      Assertions.assertSame(cause, exception.getCause());
    }
  }

  @Test
  public void testConfigTestOnBorrow() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.TEST_ON_BORROW.getKey(), "false");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertInstanceOf(BasicDataSource.class, dataSource);
    Assertions.assertFalse(((BasicDataSource) dataSource).getTestOnBorrow());
    ((BasicDataSource) dataSource).close();
  }

  @Test
  public void testConfigMaxIdle() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "16");
    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "12");

    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      Assertions.assertEquals(16, dataSource.getMaxTotal());
      Assertions.assertEquals(12, dataSource.getMaxIdle());
    }

    properties.remove(JdbcConfig.POOL_MAX_IDLE.getKey());
    properties.put("maxIdle", "7");
    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      Assertions.assertEquals(7, dataSource.getMaxIdle());
    }

    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "12");
    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      Assertions.assertEquals(12, dataSource.getMaxIdle());
    }

    properties.remove(JdbcConfig.POOL_MAX_IDLE.getKey());
    properties.put("maxIdle", "-1");
    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      Assertions.assertEquals(-1, dataSource.getMaxIdle());
      Assertions.assertEquals(2, dataSource.getMinIdle());
    }

    // An idle limit below the minimum pool size also caps minIdle.
    properties.remove("maxIdle");
    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "1");
    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      Assertions.assertEquals(1, dataSource.getMaxIdle());
      Assertions.assertEquals(1, dataSource.getMinIdle());
    }
  }

  @Test
  public void testRetainsConnectionsAcrossReadBursts() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "16");
    properties.put(JdbcConfig.POOL_MAX_IDLE.getKey(), "12");

    try (BasicDataSource dataSource =
        (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
      dataSource.setAccessToUnderlyingConnectionAllowed(true);
      Set<Connection> physicalConnections = Collections.newSetFromMap(new IdentityHashMap<>());
      for (int round = 0; round < 5; round++) {
        List<Connection> borrowed = new ArrayList<>();
        try {
          for (int i = 0; i < 12; i++) {
            Connection connection = dataSource.getConnection();
            borrowed.add(connection);
            physicalConnections.add(((DelegatingConnection<?>) connection).getInnermostDelegate());
            try (Statement statement = connection.createStatement()) {
              statement.execute("SELECT 1");
            }
          }
        } finally {
          for (Connection connection : borrowed) {
            connection.close();
          }
        }
        Assertions.assertEquals(12, dataSource.getNumIdle());
      }
      Assertions.assertFalse(physicalConnections.contains(null));
      Assertions.assertEquals(12, physicalConnections.size());
    }
  }

  @Test
  public void testCloseDoesNotThrow() {
    JdbcCatalogOperations catalogOperations =
        new JdbcCatalogOperations(
            new SqliteExceptionConverter(),
            new SqliteTypeConverter(),
            new SqliteDatabaseOperations("/illegal/path"),
            new SqliteTableOperations(),
            new SqliteColumnDefaultValueConverter());

    Assertions.assertDoesNotThrow(catalogOperations::close);
  }
}
