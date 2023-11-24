/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.catalog.rel.BaseColumn;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJdbcTableOperations {

  private static DataSource DATA_SOURCE;
  private static JdbcExceptionConverter EXCEPTION_CONVERTER;

  private static SqliteTypeConverter TYPE_CONVERTER;

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
    createJdbcDatabaseOperations();
  }

  @AfterAll
  public static void stop() {
    FileUtils.deleteQuietly(BASE_FILE_DIR);
  }

  private static void createTypeConverter() {
    TYPE_CONVERTER = new SqliteTypeConverter();
  }

  private static void createExceptionConverter() {
    EXCEPTION_CONVERTER = new SqliteExceptionConverter();
  }

  private static void createDataSource() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_URL.getKey(), JDBC_URL);
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    DATA_SOURCE = DataSourceUtils.createDataSource(properties);
  }

  private static void createJdbcDatabaseOperations() {
    JDBC_TABLE_OPERATIONS = new SqliteTableOperations();
    JDBC_TABLE_OPERATIONS.initialize(DATA_SOURCE, EXCEPTION_CONVERTER, TYPE_CONVERTER);
  }

  @Test
  public void testOperationTable() {
    String table1 = "table1";
    JdbcColumn[] columns = generateRandomColumn(1, 4);
    // Sqlite does not support the comment and default value attribute, so it is not set here
    JdbcColumn col_a =
        new JdbcColumn.Builder()
            .withName("col_a")
            .withNullable(true)
            .withType(Types.IntegerType.get())
            .withComment(null)
            .withDefaultValue(null)
            .build();
    JdbcColumn col_b =
        new JdbcColumn.Builder()
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
                DATABASE_NAME, table1, jdbcColumns, null, properties, null));

    // list table.
    List<String> allTables = JDBC_TABLE_OPERATIONS.list(DATABASE_NAME);
    Assertions.assertEquals(1, allTables.size());
    Assertions.assertEquals(table1, allTables.get(0));

    // load table.
    JdbcTable loadTable = JDBC_TABLE_OPERATIONS.load(DATABASE_NAME, table1);
    Assertions.assertNotNull(loadTable);
    Assertions.assertEquals(table1, loadTable.name());
    Assertions.assertEquals(null, loadTable.comment());
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
      Assertions.assertEquals(jdbcColumn.nullable(), ((JdbcColumn) column).nullable());
      Assertions.assertEquals(
          jdbcColumn.getDefaultValue(), ((JdbcColumn) column).getDefaultValue());
    }

    String newName = "table2";

    Assertions.assertDoesNotThrow(
        () -> JDBC_TABLE_OPERATIONS.rename(DATABASE_NAME, table1, newName));

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> JDBC_TABLE_OPERATIONS.rename(DATABASE_NAME, "no_exist", newName));
    allTables = JDBC_TABLE_OPERATIONS.list(DATABASE_NAME);
    Assertions.assertEquals(newName, allTables.get(0));

    // Sqlite does not support modifying the column type of table
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            JDBC_TABLE_OPERATIONS.alterTable(
                DATABASE_NAME,
                newName,
                TableChange.updateColumnType(new String[] {col_a.name()}, Types.StringType.get())));

    // delete table.
    JDBC_TABLE_OPERATIONS.drop(DATABASE_NAME, newName);
    allTables = JDBC_TABLE_OPERATIONS.list(DATABASE_NAME);
    Assertions.assertEquals(0, allTables.size());
  }

  private static JdbcColumn[] generateRandomColumn(int minSize, int maxSize) {
    String prefixColName = "col_";
    JdbcColumn[] columns = new JdbcColumn[RandomUtils.nextInt(minSize, maxSize)];
    for (int j = 0; j < columns.length; j++) {
      columns[j] =
          new JdbcColumn.Builder()
              .withName(prefixColName + (j + 1))
              .withNullable(RandomUtils.nextBoolean())
              .withType(getRandomGravitinoType())
              .build();
    }
    return columns;
  }

  private static Type getRandomGravitinoType() {
    Collection<Type> values = TYPE_CONVERTER.getGravitinoTypes();
    return values.stream()
        .skip(RandomUtils.nextInt(0, values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }
}
