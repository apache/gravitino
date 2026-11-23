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

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.connector.BaseColumn;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestJdbcTableOperations {

  private static DataSource DATA_SOURCE;
  private static JdbcExceptionConverter EXCEPTION_CONVERTER;

  private static SqliteTypeConverter TYPE_CONVERTER;

  private static SqliteColumnDefaultValueConverter COLUMN_DEFAULT_VALUE_CONVERTER;

  private static SqliteTableOperations JDBC_TABLE_OPERATIONS;

  private static File BASE_FILE_DIR;

  private static String FILE_PATH;
  private static String JDBC_URL;
  private static String DATABASE_NAME = "test";

  @BeforeAll
  public static void startup() throws IOException {
    BASE_FILE_DIR = Files.createTempDirectory("gravitino-jdbc").toFile();
    FILE_PATH = BASE_FILE_DIR.getPath() + "/" + DATABASE_NAME;
    JDBC_URL = "jdbc:sqlite:" + FILE_PATH;
    FileUtils.createParentDirectories(new File(FILE_PATH));
    createDataSource();
    createExceptionConverter();
    createTypeConverter();
    createColumnDefaultValueConverter();
    createJdbcDatabaseOperations();
  }

  @AfterAll
  public static void stop() {
    FileUtils.deleteQuietly(BASE_FILE_DIR);
  }

  private static void createTypeConverter() {
    TYPE_CONVERTER = new SqliteTypeConverter();
  }

  private static void createColumnDefaultValueConverter() {
    COLUMN_DEFAULT_VALUE_CONVERTER = new SqliteColumnDefaultValueConverter();
  }

  private static void createExceptionConverter() {
    EXCEPTION_CONVERTER = new SqliteExceptionConverter();
  }

  private static void createDataSource() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), JDBC_URL);
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    DATA_SOURCE = DataSourceUtils.createDataSource(properties);
  }

  private static void createJdbcDatabaseOperations() {
    JDBC_TABLE_OPERATIONS = new SqliteTableOperations();
    JDBC_TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        EXCEPTION_CONVERTER,
        TYPE_CONVERTER,
        COLUMN_DEFAULT_VALUE_CONVERTER,
        Collections.emptyMap());
  }

  @Test
  void testRequestLocalMetadataEnrichment() throws Exception {
    String tableName = "snapshot_hooks";
    JdbcTableOperations operations = Mockito.spy(JDBC_TABLE_OPERATIONS);
    try (Connection setup = DATA_SOURCE.getConnection();
        Statement statement = setup.createStatement()) {
      statement.executeUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
      try {
        List<Connection> borrowed = new ArrayList<>();
        JdbcTable first =
            operations.load(
                DATABASE_NAME,
                tableName,
                (connection, builder) -> {
                  Assertions.assertFalse(Assertions.assertDoesNotThrow(connection::isClosed));
                  borrowed.add(connection);
                  builder.withProperties(Map.of("snapshot", "first"));
                });
        JdbcTable second =
            operations.load(
                DATABASE_NAME,
                tableName,
                (connection, builder) -> {
                  Assertions.assertFalse(Assertions.assertDoesNotThrow(connection::isClosed));
                  borrowed.add(connection);
                  builder.withProperties(Map.of("snapshot", "second"));
                });
        Assertions.assertEquals("first", first.properties().get("snapshot"));
        Assertions.assertEquals("second", second.properties().get("snapshot"));
        Assertions.assertEquals("id", first.columns()[0].name());
        for (Connection connection : borrowed) {
          Assertions.assertTrue(connection.isClosed());
        }
        Mockito.verify(operations, Mockito.never())
            .getTablePartitioning(Mockito.any(), Mockito.anyString(), Mockito.anyString());
        Mockito.verify(operations, Mockito.never())
            .getTableProperties(Mockito.any(), Mockito.anyString());
        Mockito.verify(operations, Mockito.never())
            .correctJdbcTableFields(
                Mockito.any(), Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.verify(operations, Mockito.times(2))
            .getDistributionInfo(Mockito.any(), Mockito.eq(DATABASE_NAME), Mockito.eq(tableName));
        JdbcTable standard = operations.load(DATABASE_NAME, tableName);
        Assertions.assertFalse(standard.properties().containsKey("snapshot"));
        Mockito.verify(operations)
            .getTablePartitioning(Mockito.any(), Mockito.eq(DATABASE_NAME), Mockito.eq(tableName));
        Mockito.verify(operations).getTableProperties(Mockito.any(), Mockito.eq(tableName));
        Mockito.verify(operations)
            .correctJdbcTableFields(
                Mockito.any(), Mockito.eq(DATABASE_NAME), Mockito.eq(tableName), Mockito.any());
      } finally {
        statement.executeUpdate("DROP TABLE " + tableName);
      }
    }
  }

  @Test
  void testEnrichmentFailureClosesConnectionAndPreservesCause() throws Exception {
    String tableName = "snapshot_failure";
    try (Connection setup = DATA_SOURCE.getConnection();
        Statement statement = setup.createStatement()) {
      statement.executeUpdate("CREATE TABLE " + tableName + " (id INTEGER)");
      try {
        List<Connection> borrowed = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("snapshot failed");
        Assertions.assertSame(
            failure,
            Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                    JDBC_TABLE_OPERATIONS.load(
                        DATABASE_NAME,
                        tableName,
                        (connection, builder) -> {
                          Assertions.assertFalse(
                              Assertions.assertDoesNotThrow(connection::isClosed));
                          borrowed.add(connection);
                          throw failure;
                        })));
        Assertions.assertEquals(1, borrowed.size());
        Assertions.assertTrue(borrowed.get(0).isClosed());
      } finally {
        statement.executeUpdate("DROP TABLE " + tableName);
      }
    }
  }

  @Test
  void testTargetValidationAfterSqlValidationBeforeExecutionConnection() throws Exception {
    JdbcTableOperations operations = Mockito.spy(new SqliteTableOperations());
    DataSource source = Mockito.mock(DataSource.class);
    operations.initialize(
        source, EXCEPTION_CONVERTER, TYPE_CONVERTER, COLUMN_DEFAULT_VALUE_CONVERTER, Map.of());
    TableChange change = TableChange.deleteColumn(new String[] {"id"}, false);
    IllegalArgumentException invalid = new IllegalArgumentException("invalid local SQL");
    Mockito.doThrow(invalid)
        .when(operations)
        .generateAlterTableSql(DATABASE_NAME, "target", change);
    Assertions.assertSame(
        invalid,
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> operations.alterTable(DATABASE_NAME, "target", change)));
    Mockito.verify(operations, Mockito.never())
        .validateAlterTableTarget(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn("").when(operations).generateAlterTableSql(DATABASE_NAME, "target", change);
    operations.alterTable(DATABASE_NAME, "target", change);
    Mockito.verify(operations, Mockito.never())
        .validateAlterTableTarget(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn("ALTER TABLE target DROP COLUMN id")
        .when(operations)
        .generateAlterTableSql(DATABASE_NAME, "target", change);
    IllegalStateException forbidden = new IllegalStateException("unsupported target kind");
    Mockito.doThrow(forbidden).when(operations).validateAlterTableTarget(DATABASE_NAME, "target");
    Assertions.assertSame(
        forbidden,
        Assertions.assertThrows(
            IllegalStateException.class,
            () -> operations.alterTable(DATABASE_NAME, "target", change)));
    Mockito.verify(operations).validateAlterTableTarget(DATABASE_NAME, "target");
    Mockito.verifyNoInteractions(source);
  }

  @Test
  public void testOperationTable() {
    String table1 = "table1";
    JdbcColumn[] columns = generateRandomColumn(1, 4);
    // Sqlite does not support the comment and default value attribute, so it is not set here
    JdbcColumn col_a =
        JdbcColumn.builder()
            .withName("col_a")
            .withNullable(true)
            .withType(Types.IntegerType.get())
            .withComment(null)
            .withDefaultValue(null)
            .build();
    JdbcColumn col_b =
        JdbcColumn.builder()
            .withName("col_b")
            .withNullable(false)
            .withType(Types.StringType.get())
            .withComment(null)
            .withDefaultValue(null)
            .build();
    columns = ArrayUtils.add(columns, col_a);
    JdbcColumn[] jdbcColumns = ArrayUtils.add(columns, col_b);
    HashMap<String, String> properties = Maps.newHashMap();

    // create table.
    Assertions.assertDoesNotThrow(
        () ->
            JDBC_TABLE_OPERATIONS.create(
                DATABASE_NAME,
                table1,
                jdbcColumns,
                null,
                properties,
                null,
                Distributions.NONE,
                Indexes.EMPTY_INDEXES));

    // list table.
    List<String> allTables = JDBC_TABLE_OPERATIONS.listTables(DATABASE_NAME);
    Assertions.assertEquals(1, allTables.size());
    Assertions.assertEquals(table1, allTables.get(0));

    // load table.
    JdbcTable loadTable = JDBC_TABLE_OPERATIONS.load(DATABASE_NAME, table1);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(table1, loadTable.name());
    Assertions.assertNull(loadTable.comment());
    Assertions.assertEquals(properties, loadTable.properties());
    Assertions.assertEquals(jdbcColumns.length, loadTable.columns().length);
    Map<String, JdbcColumn> createColumnMap =
        Arrays.stream(jdbcColumns).collect(Collectors.toMap(BaseColumn::name, v -> v));
    for (Column column : loadTable.columns()) {
      Assertions.assertTrue(createColumnMap.containsKey(column.name()));
      JdbcColumn jdbcColumn = createColumnMap.get(column.name());
      Assertions.assertEquals(jdbcColumn.name(), column.name());
      Assertions.assertEquals(jdbcColumn.comment(), column.comment());
      Assertions.assertEquals(jdbcColumn.dataType(), column.dataType());
      Assertions.assertEquals(jdbcColumn.nullable(), column.nullable());
      Assertions.assertEquals(jdbcColumn.defaultValue(), column.defaultValue());
    }

    String newName = "table2";

    Assertions.assertDoesNotThrow(
        () -> JDBC_TABLE_OPERATIONS.rename(DATABASE_NAME, table1, newName));

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> JDBC_TABLE_OPERATIONS.rename(DATABASE_NAME, "no_exist", newName));
    allTables = JDBC_TABLE_OPERATIONS.listTables(DATABASE_NAME);
    Assertions.assertEquals(newName, allTables.get(0));

    TableChange tableChange =
        TableChange.updateColumnType(new String[] {col_a.name()}, Types.StringType.get());
    // Sqlite does not support modifying the column type of table
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> JDBC_TABLE_OPERATIONS.alterTable(DATABASE_NAME, newName, tableChange));

    // delete table.
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.drop(DATABASE_NAME, newName), "table should be dropped");
    allTables = JDBC_TABLE_OPERATIONS.listTables(DATABASE_NAME);
    Assertions.assertEquals(0, allTables.size());

    // delete non-existent table.
    Assertions.assertFalse(
        JDBC_TABLE_OPERATIONS.drop(DATABASE_NAME, newName), "table should be non-existent");
  }

  @Test
  public void testDriverVersionParsing() {
    // Test supported MySQL versions
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-8.0.16"));
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-8.0.19"));
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-8.1.0"));
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-9.0.0"));

    // Test unsupported MySQL versions
    Assertions.assertFalse(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-8.0.15"));
    Assertions.assertFalse(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-8.0.11"));
    Assertions.assertFalse(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-5.1.24"));

    // Test invalid MySQL formats
    Assertions.assertFalse(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("mysql-connector-java-invalid"));
    Assertions.assertFalse(JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported(""));
    Assertions.assertFalse(JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported(null));

    // Test non-MySQL drivers (should be supported)
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("com.oceanbase.jdbc.Driver"));
    Assertions.assertTrue(
        JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("org.postgresql.Driver"));
    Assertions.assertTrue(JDBC_TABLE_OPERATIONS.isMySQLDriverVersionSupported("org.sqlite.JDBC"));
  }

  @Test
  public void testWithPendingDefaultValueAndNullability() {
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withComment("comment")
            .withNullable(true)
            .withDefaultValue(Literals.NULL)
            .build();
    String[] fieldName = {"col_1"};

    // A NULL default is dropped when the column becomes not null.
    JdbcColumn pending =
        JdbcTableOperations.withPendingDefaultValueAndNullability(
            column, TableChange.updateColumnNullability(fieldName, false));
    Assertions.assertFalse(pending.nullable());
    Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, pending.defaultValue());
    Assertions.assertEquals("comment", pending.comment());
    Assertions.assertEquals(Types.IntegerType.get(), pending.dataType());

    // A new default in the same request is kept, whichever order the changes come in.
    TableChange setDefault =
        TableChange.updateColumnDefaultValue(fieldName, Literals.integerLiteral(5));
    TableChange setNotNull = TableChange.updateColumnNullability(fieldName, false);
    for (TableChange[] changes :
        new TableChange[][] {{setDefault, setNotNull}, {setNotNull, setDefault}}) {
      pending = JdbcTableOperations.withPendingDefaultValueAndNullability(column, changes);
      Assertions.assertFalse(pending.nullable());
      Assertions.assertEquals(Literals.integerLiteral(5), pending.defaultValue());
    }

    // Changes to other columns are ignored.
    pending =
        JdbcTableOperations.withPendingDefaultValueAndNullability(
            column,
            TableChange.updateColumnNullability(new String[] {"col_2"}, false),
            TableChange.updateColumnDefaultValue(
                new String[] {"col_2"}, Literals.integerLiteral(5)));
    Assertions.assertTrue(pending.nullable());
    Assertions.assertEquals(Literals.NULL, pending.defaultValue());

    // A NULL default requested for a column that ends up not null is rejected.
    TableChange setNullDefault = TableChange.updateColumnDefaultValue(fieldName, Literals.NULL);
    for (TableChange[] changes :
        new TableChange[][] {{setNullDefault, setNotNull}, {setNotNull, setNullDefault}}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> JdbcTableOperations.withPendingDefaultValueAndNullability(column, changes));
    }
    JdbcColumn notNullColumn =
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withDefaultValue(Literals.integerLiteral(5))
            .build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            JdbcTableOperations.withPendingDefaultValueAndNullability(
                notNullColumn, setNullDefault));
  }

  private static JdbcColumn[] generateRandomColumn(int minSize, int maxSize) {
    Random r = new Random();
    String prefixColName = "col_";
    JdbcColumn[] columns = new JdbcColumn[r.nextInt(maxSize - minSize) + minSize];
    for (int j = 0; j < columns.length; j++) {
      columns[j] =
          JdbcColumn.builder()
              .withName(prefixColName + (j + 1))
              .withNullable(r.nextBoolean())
              .withType(getRandomGravitinoType())
              .build();
    }
    return columns;
  }

  private static Type getRandomGravitinoType() {
    Collection<Type> values = TYPE_CONVERTER.getGravitinoTypes();
    Random r = new Random();
    return values.stream()
        .skip(r.nextInt(values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }
}
