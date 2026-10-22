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
package org.apache.gravitino.catalog.oceanbase.operation;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseTypeConverter;
import org.apache.gravitino.rel.TableChange;
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

public class TestOceanBaseTableOperationsSqlGeneration {

  private static class TestableOceanBaseTableOperations extends OceanBaseTableOperations {
    public TestableOceanBaseTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new OceanBaseTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
    }

    private TestableOceanBaseTableOperations(ConnectionCountingDataSource connections) {
      this();
      dataSource = connections.dataSource();
    }

    public String createTableSql(String tableName, JdbcColumn[] columns) {
      return generateCreateTableSql(
          tableName,
          columns,
          "comment",
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);
    }
  }

  @ParameterizedTest
  @MethodSource("comments")
  void testCreateEscapesTableAndColumnComments(String comment, String escaped) {
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
    String sql =
        ops.generateCreateTableSql(
            "test_table",
            new JdbcColumn[] {column(comment)},
            comment,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);

    Assertions.assertTrue(sql.contains("COMMENT '" + escaped + "' "), sql);
    Assertions.assertTrue(sql.endsWith("COMMENT='" + escaped + "';"), sql);
  }

  @ParameterizedTest
  @MethodSource("comments")
  void testUpdateTableCommentEscapesLiteral(String comment, String escaped) {
    TestableOceanBaseTableOperations ops = operationsWithTable();
    String sql =
        ops.generateAlterTableSql("database", "test_table", TableChange.updateComment(comment));

    Assertions.assertEquals("ALTER TABLE `test_table`\nCOMMENT '" + escaped + "';", sql);
  }

  @Test
  void testClearTableCommentWithNull() {
    TestableOceanBaseTableOperations ops = operationsWithTable();
    String sql =
        ops.generateAlterTableSql("database", "test_table", TableChange.updateComment(null));

    Assertions.assertEquals("ALTER TABLE `test_table`\nCOMMENT '';", sql);
  }

  @ParameterizedTest
  @MethodSource("comments")
  void testAddColumnEscapesComment(String comment, String escaped) {
    TestableOceanBaseTableOperations ops = operationsWithTable();
    String sql =
        ops.generateAlterTableSql(
            "database",
            "test_table",
            TableChange.addColumn(new String[] {"new_col"}, Types.IntegerType.get(), comment));

    Assertions.assertTrue(sql.contains("COMMENT '" + escaped + "' "), sql);
  }

  @ParameterizedTest
  @MethodSource("comments")
  void testModifyColumnEscapesComment(String comment, String escaped) {
    TestableOceanBaseTableOperations ops = operationsWithTable();
    String sql =
        ops.generateAlterTableSql(
            "database",
            "test_table",
            TableChange.updateColumnComment(new String[] {"col1"}, comment));

    Assertions.assertTrue(sql.contains("MODIFY COLUMN `col1`"), sql);
    Assertions.assertTrue(sql.contains("COMMENT '" + escaped + "' "), sql);
  }

  @Test
  void testPurgeIsRejectedWithoutExecutingSql() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations(connections);

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> ops.purge("database", "test_table"));

    Assertions.assertEquals(
        "OceanBase does not support purge table in Gravitino, please use drop table",
        exception.getMessage());
    Assertions.assertTrue(connections.executedSql().isEmpty());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  void testDropStillExecutesDropTable() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations(connections);

    Assertions.assertTrue(ops.drop("database", "test_table"));

    Assertions.assertEquals(List.of("DROP TABLE `test_table`"), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
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
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
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

  private static JdbcColumn column(String comment) {
    return JdbcColumn.builder()
        .withName("col1")
        .withType(Types.IntegerType.get())
        .withNullable(true)
        .withComment(comment)
        .build();
  }

  private static TestableOceanBaseTableOperations operationsWithTable() {
    JdbcTable table =
        JdbcTable.builder()
            .withName("test_table")
            .withComment("old comment")
            .withColumns(new JdbcColumn[] {column("old comment")})
            .build();
    return new TestableOceanBaseTableOperations() {
      /** Returns the table fixture without accessing a database. */
      @Override
      public JdbcTable load(String databaseName, String tableName) {
        Assertions.assertEquals("database", databaseName);
        Assertions.assertEquals("test_table", tableName);
        return table;
      }
    };
  }

  private static Stream<Arguments> comments() {
    return Stream.of(
        Arguments.of("customer's id", "customer''s id"),
        Arguments.of("C:\\new\\trailing\\", "C:\\\\new\\\\trailing\\\\"),
        Arguments.of("backslash\\'quote; --", "backslash\\\\''quote; --"),
        Arguments.of("line1\n中文 \"double\"", "line1\n中文 \"double\""));
  }
}
