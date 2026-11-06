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
package org.apache.gravitino.catalog.postgresql.operation;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class TestPostgreSqlSchemaOperations extends TestPostgreSql {

  @Test
  void testCascadeDropPreservesExternalDependents() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_source");
    String other = RandomNameUtils.genRandomName("cascade_other");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + schema);
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + other);
      try {
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE TABLE " + schema + ".parent (id int PRIMARY KEY, payload text)");
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE VIEW " + other + ".v AS SELECT * FROM " + schema + ".parent");
        JdbcConnectorUtils.executeUpdate(
            connection,
            "CREATE TABLE " + other + ".child (id int REFERENCES " + schema + ".parent)");
        NonEmptySchemaException error =
            Assertions.assertThrows(
                NonEmptySchemaException.class, () -> DATABASE_OPERATIONS.delete(schema, true));
        Assertions.assertTrue(error.getMessage().contains(schema));
        Assertions.assertEquals("2BP01", ((SQLException) error.getCause()).getSQLState());
        try (Statement statement = connection.createStatement();
            ResultSet rows = statement.executeQuery("SELECT * FROM " + other + ".v")) {
          Assertions.assertFalse(rows.next());
        }
        Assertions.assertThrows(
            SQLException.class,
            () ->
                JdbcConnectorUtils.executeUpdate(
                    connection, "INSERT INTO " + other + ".child VALUES (123)"));
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA " + other + " CASCADE");
        Assertions.assertThrows(
            NonEmptySchemaException.class, () -> DATABASE_OPERATIONS.delete(schema, true));
        JdbcConnectorUtils.executeUpdate(connection, "DROP TABLE " + schema + ".parent");
        Assertions.assertTrue(DATABASE_OPERATIONS.delete(schema, true));
      } finally {
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA IF EXISTS " + other + " CASCADE");
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void testCascadeDropPreservesExternalSequenceDefault() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_sequence");
    String other = RandomNameUtils.genRandomName("cascade_default");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + schema);
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + other);
      try {
        JdbcConnectorUtils.executeUpdate(connection, "CREATE SEQUENCE " + schema + ".seq");
        JdbcConnectorUtils.executeUpdate(
            connection,
            "CREATE TABLE " + other + ".child (id bigint DEFAULT nextval('" + schema + ".seq'))");
        NonEmptySchemaException error =
            Assertions.assertThrows(
                NonEmptySchemaException.class, () -> DATABASE_OPERATIONS.delete(schema, true));
        Assertions.assertTrue(error.getMessage().contains(schema));
        Assertions.assertEquals("2BP01", ((SQLException) error.getCause()).getSQLState());
        JdbcConnectorUtils.executeUpdate(
            connection, "INSERT INTO " + other + ".child DEFAULT VALUES");
        try (Statement statement = connection.createStatement();
            ResultSet rows = statement.executeQuery("SELECT id FROM " + other + ".child")) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(1L, rows.getLong(1));
          Assertions.assertFalse(rows.wasNull());
        }
        Assertions.assertEquals(schema, DATABASE_OPERATIONS.load(schema).name());
      } finally {
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA IF EXISTS " + other + " CASCADE");
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void testCascadeDropPreservesContainedToastObjects() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_toast");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + schema);
      try {
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE TABLE " + schema + ".payload (id int PRIMARY KEY, body text)");
        long toastId;
        try (Statement statement = connection.createStatement();
            ResultSet rows =
                statement.executeQuery(
                    "SELECT reltoastrelid FROM pg_catalog.pg_class WHERE oid = '"
                        + schema
                        + ".payload'::regclass")) {
          Assertions.assertTrue(rows.next());
          toastId = rows.getLong(1);
          Assertions.assertTrue(toastId > 0, "The test table must have an internal TOAST table");
        }

        Assertions.assertThrows(
            NonEmptySchemaException.class, () -> DATABASE_OPERATIONS.delete(schema, true));
        Assertions.assertEquals(schema, DATABASE_OPERATIONS.load(schema).name());
        try (Statement statement = connection.createStatement();
            ResultSet rows =
                statement.executeQuery(
                    "SELECT COUNT(*) FROM pg_catalog.pg_class WHERE oid = " + toastId)) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(1, rows.getInt(1));
        }
      } finally {
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void testSchemaOwnerDoesNotNeedTableLockPrivileges() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_owner");
    String owner = RandomNameUtils.genRandomName("schema_owner");
    try (Connection connection = DATA_SOURCE.getConnection();
        Connection ownerConnection =
            DriverManager.getConnection(
                CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword())) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE ROLE " + owner);
      JdbcConnectorUtils.executeUpdate(
          connection, "CREATE SCHEMA " + schema + " AUTHORIZATION " + owner);
      JdbcConnectorUtils.executeUpdate(connection, "CREATE TABLE " + schema + ".t (id int)");
      JdbcConnectorUtils.executeUpdate(ownerConnection, "SET ROLE " + owner);
      PostgreSqlSchemaOperations operations =
          new PostgreSqlSchemaOperations() {
            @Override
            protected Connection getConnection() throws SQLException {
              Connection result =
                  DriverManager.getConnection(
                      CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
              JdbcConnectorUtils.executeUpdate(result, "SET ROLE " + owner);
              return result;
            }
          };
      operations.initialize(
          DATA_SOURCE,
          JDBC_EXCEPTION_CONVERTER,
          Map.of(JdbcConfig.JDBC_DATABASE.getKey(), CONTAINER.getDatabaseName()));
      try {
        NonEmptySchemaException error =
            Assertions.assertThrows(
                NonEmptySchemaException.class, () -> operations.delete(schema, true));
        Assertions.assertEquals("2BP01", ((SQLException) error.getCause()).getSQLState());
        JdbcConnectorUtils.executeUpdate(ownerConnection, "DROP TABLE " + schema + ".t RESTRICT");
        Assertions.assertTrue(operations.delete(schema, true));
        Assertions.assertFalse(operations.delete(schema, true));
      } finally {
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        JdbcConnectorUtils.executeUpdate(connection, "DROP ROLE " + owner);
      }
    }
  }

  @Test
  void testCascadeDropPreservesExternalAutomaticDependents() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_auto");
    String other = RandomNameUtils.genRandomName("cascade_external");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + schema);
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + other);
      try {
        JdbcConnectorUtils.executeUpdate(
            connection,
            "CREATE TABLE " + schema + ".parent (id int, v int) PARTITION BY RANGE(id)");
        JdbcConnectorUtils.executeUpdate(
            connection,
            "CREATE TABLE "
                + other
                + ".part PARTITION OF "
                + schema
                + ".parent FOR VALUES FROM (0) TO (100)");
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE STATISTICS " + other + ".st ON id, v FROM " + schema + ".parent");
        Assertions.assertThrows(
            NonEmptySchemaException.class, () -> DATABASE_OPERATIONS.delete(schema, true));
        JdbcConnectorUtils.executeUpdate(connection, "INSERT INTO " + other + ".part VALUES (1,2)");
        try (Statement statement = connection.createStatement();
            ResultSet rows =
                statement.executeQuery(
                    "SELECT COUNT(*) FROM pg_catalog.pg_statistic_ext WHERE stxname = 'st' AND "
                        + "stxnamespace = '"
                        + other
                        + "'::regnamespace")) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(1, rows.getInt(1));
        }
      } finally {
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA IF EXISTS " + other + " CASCADE");
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void testDependencyCommittedImmediatelyBeforeDropIsPreserved() throws SQLException {
    String schema = RandomNameUtils.genRandomName("cascade_race");
    String other = RandomNameUtils.genRandomName("cascade_race_other");
    AtomicBoolean created = new AtomicBoolean();
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + schema);
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + other);
      JdbcConnectorUtils.executeUpdate(
          connection, "CREATE VIEW " + schema + ".v AS SELECT 1 AS id");
      PostgreSqlSchemaOperations operations =
          new PostgreSqlSchemaOperations() {
            @Override
            public String generateDropDatabaseSql(String name, boolean cascade) {
              try {
                // A different connection commits the dependency after the old preflight would
                // have finished and immediately before the actual DROP statement is generated.
                JdbcConnectorUtils.executeUpdate(
                    connection, "CREATE VIEW " + other + ".v AS SELECT * FROM " + schema + ".v");
                created.set(true);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
              return super.generateDropDatabaseSql(name, cascade);
            }
          };
      operations.initialize(
          DATA_SOURCE,
          JDBC_EXCEPTION_CONVERTER,
          Map.of(JdbcConfig.JDBC_DATABASE.getKey(), CONTAINER.getDatabaseName()));
      try {
        Assertions.assertThrows(
            NonEmptySchemaException.class, () -> operations.delete(schema, true));
        Assertions.assertTrue(created.get());
        try (Statement statement = connection.createStatement();
            ResultSet rows = statement.executeQuery("SELECT id FROM " + other + ".v")) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(1, rows.getInt(1));
        }
        Assertions.assertEquals(schema, DATABASE_OPERATIONS.load(schema).name());
      } finally {
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA IF EXISTS " + other + " CASCADE");
        JdbcConnectorUtils.executeUpdate(
            connection, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
      }
    }
  }

  @Test
  void testQuotedSchemaNamesLoadWithoutExecutingSql() throws SQLException {
    for (String schema : new String[] {"quoted';CREATE SCHEMA injected_marker;--"}) {
      DATABASE_OPERATIONS.create(schema, "safe comment", Map.of());
      try {
        Assertions.assertEquals("safe comment", DATABASE_OPERATIONS.load(schema).comment());
        Assertions.assertEquals(schema, DATABASE_OPERATIONS.load(schema).name());
        Assertions.assertFalse(DATABASE_OPERATIONS.listDatabases().contains("injected_marker"));
      } finally {
        DATABASE_OPERATIONS.delete(schema, true);
      }
    }
  }

  @Test
  void testBackslashSchemaNameLoads() {
    String schema = "back\\slash_%";
    DATABASE_OPERATIONS.create(schema, "comment", Map.of());
    try {
      Assertions.assertEquals(schema, DATABASE_OPERATIONS.load(schema).name());
      Assertions.assertEquals("comment", DATABASE_OPERATIONS.load(schema).comment());
    } finally {
      DATABASE_OPERATIONS.delete(schema, true);
    }
  }

  @Test
  public void testBaseOperationSchema() {
    String databaseName = RandomNameUtils.genRandomName("ct_db");
    Map<String, String> properties = new HashMap<>();
    // PostgreSql does not support filling in comments directly when creating a table.
    String comment = null;
    List<String> initDatabases = DATABASE_OPERATIONS.listDatabases();

    ((PostgreSqlSchemaOperations) DATABASE_OPERATIONS)
        .createSysDatabaseNameSet()
        .forEach(
            sysPgDatabaseName -> Assertions.assertFalse(initDatabases.contains(sysPgDatabaseName)));

    testBaseOperation(databaseName, properties, comment);
    // delete database.
    Assertions.assertDoesNotThrow(() -> DATABASE_OPERATIONS.delete(databaseName, true));

    Assertions.assertThrows(
        NoSuchSchemaException.class, () -> DATABASE_OPERATIONS.load(databaseName));
    List<String> databases = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(databaseName));
  }

  @Test
  public void testCreateMultipleSchema() throws SQLException {
    String testDbName = RandomNameUtils.genRandomName("test_db_");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + testDbName);
    }
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    String jdbcUrl =
        StringUtils.substring(CONTAINER.getJdbcUrl(), 0, CONTAINER.getJdbcUrl().lastIndexOf("/"));
    properties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl + "/" + testDbName);
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DataSource dataSource = DataSourceUtils.createDataSource(properties);
    PostgreSqlSchemaOperations postgreSqlSchemaOperations = new PostgreSqlSchemaOperations();
    Map<String, String> config =
        new HashMap<String, String>() {
          {
            put(JdbcConfig.JDBC_DATABASE.getKey(), testDbName);
          }
        };
    postgreSqlSchemaOperations.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, config);

    String schema_1 = "schema_multiple_1";
    DATABASE_OPERATIONS.create(schema_1, null, null);

    List<String> schemaNames = postgreSqlSchemaOperations.listDatabases();
    Assertions.assertFalse(schemaNames.contains(schema_1));

    String schema_2 = "schema_multiple_2";
    postgreSqlSchemaOperations.create(schema_2, null, null);

    schemaNames = DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(schemaNames.contains(schema_2));

    postgreSqlSchemaOperations.delete(schema_2, true);

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          postgreSqlSchemaOperations.load(schema_2);
        });

    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> {
          DATABASE_OPERATIONS.load(schema_2);
        });

    postgreSqlSchemaOperations.create(schema_1, null, null);
    JdbcSchema load = postgreSqlSchemaOperations.load(schema_1);
    Assertions.assertEquals(schema_1, load.name());
  }
}
