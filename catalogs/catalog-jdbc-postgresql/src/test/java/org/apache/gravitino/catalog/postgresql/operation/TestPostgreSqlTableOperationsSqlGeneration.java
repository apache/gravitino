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

import java.util.Collections;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgreSqlTableOperationsSqlGeneration {

  private static class TestablePostgreSqlTableOperations extends PostgreSqlTableOperations {
    public TestablePostgreSqlTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new PostgreSqlTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
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

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
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
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
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
  public void testEscapeCommentsInGeneratedSql() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String injectedComment = "owner\\'s comment; DROP TABLE marker; --";
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.IntegerType.get())
            .withComment(injectedComment)
            .build();

    String createSql = ops.createTableSql("test_table", new JdbcColumn[] {column}, injectedComment);
    Assertions.assertTrue(
        createSql.contains("IS E'owner\\\\''s comment; DROP TABLE marker; --';"), createSql);

    String alterSql =
        ops.alterTableSql(
            "test_table", TableChange.updateColumnComment(new String[] {"col1"}, injectedComment));
    Assertions.assertEquals(
        "COMMENT ON COLUMN \"test_table\".\"col1\" "
            + "IS E'owner\\\\''s comment; DROP TABLE marker; --';",
        alterSql);
  }

  @Test
  public void testEscapeAddColumnCommentInGeneratedSql() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String comment = "owner\\'s \"comment\"; --";

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(new String[] {"col2"}, Types.IntegerType.get(), comment));

    Assertions.assertTrue(alterSql.contains("ADD COLUMN \"col2\""), alterSql);
    Assertions.assertTrue(
        alterSql.contains(
            "COMMENT ON COLUMN \"test_table\".\"col2\" IS E'owner\\\\''s \"comment\"; --';"),
        alterSql);
  }
}
