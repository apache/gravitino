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
package org.apache.gravitino.catalog.starrocks.operation;

import java.util.Collections;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksTableOperations;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStarRocksTableOperationsSqlGeneration {

  private static class TestableStarRocksTableOperations extends StarRocksTableOperations {

    public TestableStarRocksTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new StarRocksTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
    }

    public String createTableSql(
        String tableName, JdbcColumn[] columns, Distribution distribution) {
      return createTableSql(tableName, columns, distribution, "comment");
    }

    public String createTableSql(
        String tableName, JdbcColumn[] columns, Distribution distribution, String comment) {
      return generateCreateTableSql(
          tableName,
          columns,
          comment,
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          distribution,
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

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("", Types.VarCharType.of(255)))
            .build();

    // StarRocks requires distribution
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1}, distribution);
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT '' but was: " + sql);
  }

  @Test
  public void testCreateTableWithNonEmptyStringDefaultValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("abc", Types.VarCharType.of(255)))
            .build();

    // StarRocks requires distribution
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1}, distribution);
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT value but was: " + sql);
  }

  @Test
  public void testCreateTableWithWhitespaceDefaultValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("   ", Types.VarCharType.of(255)))
            .build();

    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));
    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1}, distribution);
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT '   ' but was: " + sql);
  }

  @Test
  public void testEscapeCommentsInGeneratedSql() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String tableComment = "owner\"; DROP TABLE marker; --";
    String columnComment = "owner's comment; DROP TABLE marker; --";
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.IntegerType.get())
            .withComment(columnComment)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    String createSql =
        ops.createTableSql("test_table", new JdbcColumn[] {column}, distribution, tableComment);
    Assertions.assertTrue(
        createSql.contains("COMMENT \"owner\"\"; DROP TABLE marker; -- "), createSql);
    Assertions.assertTrue(
        createSql.contains("COMMENT 'owner''s comment; DROP TABLE marker; --'"), createSql);

    String alterSql = ops.alterTableSql("test_table", TableChange.updateComment(tableComment));
    Assertions.assertTrue(
        alterSql.contains(
            "COMMENT = \"owner\"\"; DROP TABLE marker; -- "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid-1)\""),
        alterSql);
  }

  @Test
  public void testEscapeAddColumnCommentInGeneratedSql() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String comment = "owner\\'s \"comment\"; --";

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(new String[] {"col2"}, Types.IntegerType.get(), comment));

    Assertions.assertTrue(alterSql.contains("ADD COLUMN `col2`"), alterSql);
    Assertions.assertTrue(alterSql.contains("COMMENT 'owner\\\\''s \"comment\"; --'"), alterSql);
  }
}
