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
package org.apache.gravitino.catalog.mysql.operation;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestMysqlTableOperationsSqlGeneration {

  private static class TestableMysqlTableOperations extends MysqlTableOperations {
    public TestableMysqlTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new MysqlTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
    }

    private TestableMysqlTableOperations(ConnectionCountingDataSource connections) {
      this();
      dataSource = connections.dataSource();
    }

    public String createTableSql(String tableName, JdbcColumn[] columns) {
      return createTableSql(tableName, columns, "comment");
    }

    public String createTableSql(String tableName, JdbcColumn[] columns, String comment) {
      return generateCreateTableSql(
          tableName,
          columns,
          comment,
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);
    }

    public String alterTableSql(String tableName, TableChange... changes) {
      return generateAlterTableSql("database", tableName, changes);
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      return JdbcTable.builder().withName(tableName).build();
    }
  }

  @ParameterizedTest
  @MethodSource("invalidTableOptions")
  void testInvalidOptionsAreRejectedBeforeCreateAndAlterSql(String key, String value) {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations(connections);
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(Types.IntegerType.get()).build();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.create(
                "database",
                "test_table",
                new JdbcColumn[] {column},
                "comment",
                Collections.singletonMap(key, value),
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                Indexes.EMPTY_INDEXES));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable("database", "test_table", TableChange.setProperty(key, value)));
    Assertions.assertTrue(connections.executedSql().isEmpty());
    Assertions.assertEquals(0, connections.borrowed());
    Assertions.assertEquals(0, connections.peakBorrowed());
  }

  @Test
  void testUnsupportedSqlOptionNamesSupportedOptions() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(Types.IntegerType.get()).build();
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.generateCreateTableSql(
                    "test_table",
                    new JdbcColumn[] {column},
                    null,
                    Map.of("ROW_FORMAT", "DYNAMIC"),
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    Indexes.EMPTY_INDEXES));
    Assertions.assertTrue(
        error.getMessage().contains("Supported options: AUTO_INCREMENT, ENGINE"),
        error.getMessage());
  }

  @Test
  void testSupportedSqlOptionsAreNormalized() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(Types.IntegerType.get()).build();
    String sql =
        ops.generateCreateTableSql(
            "test_table",
            new JdbcColumn[] {column},
            null,
            Map.of("ENGINE", "InNoDB", "AUTO_INCREMENT", "+0042"),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    Assertions.assertTrue(sql.contains("ENGINE = InnoDB"), sql);
    Assertions.assertTrue(sql.contains("AUTO_INCREMENT = 42"), sql);
  }

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("", Types.VarCharType.of(255)))
            .build();

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1});
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT '' but was: " + sql);
  }

  @Test
  public void testCreateTableWithNonEmptyStringDefaultValue() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("abc", Types.VarCharType.of(255)))
            .build();

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1});
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT value but was: " + sql);
  }

  @Test
  public void testCurrentTimestampDefaultValueKeepsColumnPrecision() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();

    JdbcColumn datetime =
        JdbcColumn.builder()
            .withName("datetime_col")
            .withType(Types.TimestampType.withoutTimeZone())
            .withDefaultValue(Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP)
            .build();
    JdbcColumn datetime0 =
        JdbcColumn.builder()
            .withName("datetime_col_0")
            .withType(Types.TimestampType.withoutTimeZone(0))
            .withDefaultValue(Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP)
            .build();
    JdbcColumn datetime6 =
        JdbcColumn.builder()
            .withName("datetime_col_6")
            .withType(Types.TimestampType.withoutTimeZone(6))
            .withDefaultValue(Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP)
            .build();
    JdbcColumn timestamp3 =
        JdbcColumn.builder()
            .withName("timestamp_col_3")
            .withType(Types.TimestampType.withTimeZone(3))
            .withDefaultValue(Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP)
            .build();
    JdbcColumn upperCase =
        JdbcColumn.builder()
            .withName("upper_case_col")
            .withType(Types.TimestampType.withoutTimeZone(6))
            .withDefaultValue(FunctionExpression.of("CURRENT_TIMESTAMP"))
            .build();

    String sql =
        ops.createTableSql(
            "test_table", new JdbcColumn[] {datetime, datetime0, datetime6, timestamp3, upperCase});
    Assertions.assertTrue(
        sql.contains("`datetime_col` datetime NULL DEFAULT CURRENT_TIMESTAMP "), sql);
    Assertions.assertTrue(
        sql.contains("`datetime_col_0` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP "), sql);
    Assertions.assertTrue(
        sql.contains("`datetime_col_6` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP(6) "), sql);
    Assertions.assertTrue(
        sql.contains("`timestamp_col_3` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) "), sql);
    Assertions.assertTrue(
        sql.contains("`upper_case_col` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP(6) "), sql);

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(
                new String[] {"added_col"},
                Types.TimestampType.withoutTimeZone(6),
                Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP));
    Assertions.assertTrue(
        alterSql.contains("ADD COLUMN `added_col` datetime(6) DEFAULT CURRENT_TIMESTAMP(6) "),
        alterSql);
  }

  @Test
  public void testEscapeCommentsInGeneratedSql() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    String injectedComment = "owner's comment; DROP TABLE marker; --";
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.IntegerType.get())
            .withComment(injectedComment)
            .build();

    String createSql = ops.createTableSql("test_table", new JdbcColumn[] {column}, injectedComment);
    Assertions.assertTrue(
        createSql.contains("COMMENT 'owner''s comment; DROP TABLE marker; --'"), createSql);
    Assertions.assertTrue(
        createSql.contains("COMMENT='owner''s comment; DROP TABLE marker; --'"), createSql);

    String commentWithIdentifier =
        injectedComment + " (From Gravitino, DO NOT EDIT: gravitino.v1.uid1)";
    String alterSql =
        ops.alterTableSql("test_table", TableChange.updateComment(commentWithIdentifier));
    Assertions.assertTrue(
        alterSql.contains(
            "COMMENT 'owner''s comment; DROP TABLE marker; -- "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid1)'"),
        alterSql);
  }

  @Test
  public void testClearTableCommentWithNull() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();

    Assertions.assertEquals(
        "ALTER TABLE `test_table`\nCOMMENT '';",
        ops.alterTableSql("test_table", TableChange.updateComment(null)));
  }

  @Test
  public void testEscapeAddColumnCommentInGeneratedSql() {
    TestableMysqlTableOperations ops = new TestableMysqlTableOperations();
    String comment = "owner\\'s \"comment\"; --";

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(new String[] {"col2"}, Types.IntegerType.get(), comment));

    Assertions.assertTrue(alterSql.contains("ADD COLUMN `col2`"), alterSql);
    Assertions.assertTrue(alterSql.contains("COMMENT 'owner\\\\''s \"comment\"; --'"), alterSql);
  }

  private static Stream<Arguments> invalidTableOptions() {
    return Stream.of(
        Arguments.of("ENGINE", "ABC"),
        Arguments.of("ENGINE", "InnoDB, ROW_FORMAT=DYNAMIC"),
        Arguments.of("AUTO_INCREMENT", "1; DROP TABLE t"),
        Arguments.of("ROW_FORMAT", "DYNAMIC"),
        Arguments.of("ENGINE; DROP TABLE t", "MEMORY"),
        Arguments.of("AUTO_INCREMENT", null));
  }
}
