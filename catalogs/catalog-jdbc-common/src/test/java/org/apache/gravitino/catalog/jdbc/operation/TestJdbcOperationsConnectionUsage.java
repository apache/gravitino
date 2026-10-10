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
package org.apache.gravitino.catalog.jdbc.operation;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests that JDBC operations never hold one pooled connection while borrowing another. */
public class TestJdbcOperationsConnectionUsage {

  private static final String DATABASE = "test";
  private static final String TABLE = "orders";
  private static final String ALTER_SQL = "ALTER TABLE orders ADD COLUMN note INTEGER";

  @Test
  public void testAlterTableGeneratesSqlWithoutHoldingAConnection() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    JdbcTableOperations operations =
        borrowingGenerator(connections, borrowedDuringGeneration, ALTER_SQL);

    operations.alterTable(DATABASE, TABLE, TableChange.updateComment("comment"));

    Assertions.assertEquals(List.of(0), borrowedDuringGeneration);
    Assertions.assertEquals(1, connections.peakBorrowed());
    Assertions.assertEquals(List.of(ALTER_SQL), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  public void testAlterTableWithoutChangesExecutesNothing() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    JdbcTableOperations operations = borrowingGenerator(connections, borrowedDuringGeneration, "");

    operations.alterTable(DATABASE, TABLE, TableChange.updateComment("comment"));

    Assertions.assertEquals(List.of(0), borrowedDuringGeneration);
    Assertions.assertTrue(connections.executedSql().isEmpty());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  public void testAlterTableGeneratorFailureBorrowsNoExecutionConnection() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    JdbcTableOperations operations =
        new SqliteTableOperations() {
          @Override
          protected String generateAlterTableSql(
              String databaseName, String tableName, TableChange... changes) {
            throw new NoSuchTableException("Table %s does not exist", tableName);
          }
        };
    initialize(operations, connections.dataSource());

    NoSuchTableException exception =
        Assertions.assertThrows(
            NoSuchTableException.class,
            () -> operations.alterTable(DATABASE, TABLE, TableChange.updateComment("comment")));

    Assertions.assertTrue(exception.getMessage().contains(TABLE));
    Assertions.assertEquals(0, connections.peakBorrowed());
    Assertions.assertTrue(connections.executedSql().isEmpty());
  }

  @Test
  public void testCreateTableGeneratesSqlWithoutHoldingAConnection() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    String createSql = "CREATE TABLE orders (id INTEGER)";
    JdbcTableOperations operations =
        new SqliteTableOperations() {
          @Override
          protected String generateCreateTableSql(
              String tableName,
              JdbcColumn[] columns,
              String comment,
              Map<String, String> properties,
              Transform[] partitioning,
              Distribution distribution,
              Index[] indexes,
              SortOrder[] sortOrders) {
            borrowedDuringGeneration.add(connections.borrowed());
            // Like Doris checking its backends and version, query the server while generating.
            borrowAndRelease(this.dataSource);
            return createSql;
          }
        };
    initialize(operations, connections.dataSource());

    operations.create(
        DATABASE,
        TABLE,
        new JdbcColumn[0],
        null,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    Assertions.assertEquals(List.of(0), borrowedDuringGeneration);
    Assertions.assertEquals(1, connections.peakBorrowed());
    Assertions.assertEquals(List.of(createSql), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  public void testDropDatabaseGeneratesSqlWithoutHoldingAConnection() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    List<Integer> borrowedDuringGeneration = new ArrayList<>();
    String dropSql = "DROP DATABASE `test`";
    JdbcDatabaseOperations operations =
        new SqliteDatabaseOperations("unused") {
          @Override
          public String generateDropDatabaseSql(String databaseName, boolean cascade) {
            borrowedDuringGeneration.add(connections.borrowed());
            // Like the default non-cascading drop checking for tables, borrow while generating.
            borrowAndRelease(this.dataSource);
            return dropSql;
          }
        };
    operations.initialize(
        connections.dataSource(), new SqliteExceptionConverter(), Collections.emptyMap());

    operations.dropDatabase(DATABASE, false);

    Assertions.assertEquals(List.of(0), borrowedDuringGeneration);
    Assertions.assertEquals(1, connections.peakBorrowed());
    Assertions.assertEquals(List.of(dropSql), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  public void testAlterTableSucceedsWhenGeneratorLoadsTableFromSingleConnectionPool()
      throws IOException, SQLException {
    // The pool size reproduces the starvation deterministically: with the execution connection
    // borrowed first, the generator's load would wait for a connection that can never be returned.
    File directory = Files.createTempDirectory("gravitino-jdbc-alter").toFile();
    BasicDataSource dataSource = null;
    try {
      dataSource = singleConnectionPool(new File(directory, DATABASE).getPath());
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate("CREATE TABLE orders (id INTEGER)");
      }
      JdbcTableOperations operations = loadingGenerator(dataSource);

      operations.alterTable(DATABASE, TABLE, TableChange.updateComment("comment"));

      JdbcTable table = operations.load(DATABASE, TABLE);
      Assertions.assertArrayEquals(
          new String[] {"id", "note"},
          Arrays.stream(table.columns()).map(Column::name).toArray(String[]::new));
    } finally {
      if (dataSource != null) {
        dataSource.close();
      }
      FileUtils.deleteQuietly(directory);
    }
  }

  @Test
  public void testLoadReadsDriverVersionWithoutBorrowingAnotherConnection()
      throws IOException, SQLException {
    // MySQL, OceanBase and Doris read the driver version while parsing each datetime column, which
    // happens while load holds its connection.
    File directory = Files.createTempDirectory("gravitino-jdbc-load").toFile();
    BasicDataSource dataSource = null;
    try {
      dataSource = singleConnectionPool(new File(directory, DATABASE).getPath());
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate("CREATE TABLE orders (id INTEGER, created TEXT)");
      }
      List<String> driverVersions = new ArrayList<>();
      JdbcTableOperations operations =
          new SqliteTableOperations() {
            @Override
            public Integer calculateDatetimePrecision(String typeName, int columnSize, int scale) {
              driverVersions.add(getMySQLDriverVersion());
              return null;
            }
          };
      initialize(operations, dataSource);

      operations.load(DATABASE, TABLE);

      Assertions.assertEquals(2, driverVersions.size());
      Assertions.assertNotNull(driverVersions.get(0));
      Assertions.assertEquals(driverVersions.get(0), driverVersions.get(1));
    } finally {
      if (dataSource != null) {
        dataSource.close();
      }
      FileUtils.deleteQuietly(directory);
    }
  }

  @Test
  public void testNullDriverVersionIsCachedFromTheHeldConnection() throws SQLException {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    JdbcTableOperations operations = new SqliteTableOperations();
    initialize(operations, connections.dataSource());

    try (Connection held = connections.dataSource().getConnection()) {
      operations.cacheDriverVersion(held);
      // A null version is still cached, so later lookups never borrow while one is held.
      Assertions.assertNull(operations.getMySQLDriverVersion());
      Assertions.assertNull(operations.getMySQLDriverVersion());
    }

    Assertions.assertEquals(1, connections.totalBorrows());
    Assertions.assertEquals(0, connections.borrowed());
  }

  // Like the MySQL generator loading the original table, borrows a connection while generating.
  private static JdbcTableOperations borrowingGenerator(
      ConnectionCountingDataSource connections,
      List<Integer> borrowedDuringGeneration,
      String sql) {
    JdbcTableOperations operations =
        new SqliteTableOperations() {
          @Override
          protected String generateAlterTableSql(
              String databaseName, String tableName, TableChange... changes) {
            borrowedDuringGeneration.add(connections.borrowed());
            borrowAndRelease(this.dataSource);
            return sql;
          }
        };
    initialize(operations, connections.dataSource());
    return operations;
  }

  private static JdbcTableOperations loadingGenerator(DataSource dataSource) {
    JdbcTableOperations operations =
        new SqliteTableOperations() {
          @Override
          protected String generateAlterTableSql(
              String databaseName, String tableName, TableChange... changes) {
            load(databaseName, tableName);
            return ALTER_SQL;
          }
        };
    initialize(operations, dataSource);
    return operations;
  }

  private static void borrowAndRelease(DataSource dataSource) {
    try (Connection ignored = dataSource.getConnection()) {
      // Only the borrow matters.
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void initialize(JdbcTableOperations operations, DataSource dataSource) {
    operations.initialize(
        dataSource,
        new SqliteExceptionConverter(),
        new SqliteTypeConverter(),
        new SqliteColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  private static BasicDataSource singleConnectionPool(String path) {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite:" + path);
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put(JdbcConfig.POOL_MIN_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MAX_SIZE.getKey(), "1");
    properties.put(JdbcConfig.POOL_MAX_WAIT_MS.getKey(), "1000");
    return (BasicDataSource) DataSourceUtils.createDataSource(properties);
  }
}
