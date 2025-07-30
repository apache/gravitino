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
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
