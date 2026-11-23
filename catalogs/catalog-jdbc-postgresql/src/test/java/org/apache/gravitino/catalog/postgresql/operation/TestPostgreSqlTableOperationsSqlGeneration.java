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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestPostgreSqlTableOperationsSqlGeneration {

  private static class TestablePostgreSqlTableOperations extends PostgreSqlTableOperations {
    private final ResultSet indexMetadata = mock(ResultSet.class);

    public TestablePostgreSqlTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new PostgreSqlTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
      super.dataSource = mock(DataSource.class);
      try {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(indexMetadata);
      } catch (SQLException e) {
        throw new AssertionError(e);
      }
    }

    private TestablePostgreSqlTableOperations(PostgreSqlColumnDefaultValueConverter converter) {
      this();
      super.columnDefaultValueConverter = converter;
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
      return JdbcTable.builder()
          .withName(tableName)
          .withColumns(
              new JdbcColumn[] {
                JdbcColumn.builder()
                    .withName("col1")
                    .withType(Types.VarCharType.of(255))
                    .withNullable(true)
                    .build()
              })
          .build();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testStandaloneUniqueIndexCannotBeSilentlyIgnored(boolean ifExists) throws SQLException {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    when(ops.indexMetadata.next()).thenReturn(true);
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ops.alterTableSql("table", TableChange.deleteIndex("native_unique", ifExists)));
    Assertions.assertTrue(error.getMessage().contains("standalone unique index"));
  }

  @Test
  void testQuotedTableAndColumnNamesCannotEscapeIdentifiers() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String name = "t\"; DROP TABLE victim; --";
    String quoted = "\"t\"\"; DROP TABLE victim; --\"";
    Assertions.assertEquals("DROP TABLE " + quoted, ops.generateDropTableSql(name));
    Assertions.assertEquals(
        "ALTER TABLE " + quoted + " RENAME TO " + quoted, ops.generateRenameTableSql(name, name));
    Assertions.assertEquals(
        "ALTER TABLE " + quoted + " RENAME COLUMN " + quoted + " TO " + quoted + ";",
        ops.alterTableSql(name, TableChange.renameColumn(new String[] {name}, name)));
    Assertions.assertEquals(
        "ALTER TABLE " + quoted + " DROP CONSTRAINT " + quoted + ";",
        ops.alterTableSql(name, TableChange.deleteIndex(name, false)));
    Assertions.assertEquals(
        quoted, PostgreSqlTableOperations.getIndexFieldStr(new String[][] {{name}}));
    String sql =
        ops.createTableSql(
            name,
            new JdbcColumn[] {
              JdbcColumn.builder()
                  .withName(name)
                  .withType(Types.IntegerType.get())
                  .withNullable(true)
                  .build()
            });
    Assertions.assertTrue(sql.startsWith("CREATE TABLE " + quoted + " ("));
    Assertions.assertTrue(sql.contains("    " + quoted + " int4"));
  }

  @Test
  void testOversizedIndexNamesAreRejectedOnCreateAndAlter() {
    String name = "表".repeat(22);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgreSqlTableOperations.addIndexDefinition(
                "t",
                (TableChange.AddIndex)
                    TableChange.addIndex(IndexType.UNIQUE_KEY, name, new String[][] {{"col1"}})));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgreSqlTableOperations.appendIndexesSql(
                new Index[] {Indexes.unique(name, new String[][] {{"col1"}})},
                new StringBuilder()));
  }

  @Test
  void testUnnamedIndexesKeepNativeTableAndColumnQuotes() {
    for (String name : new String[] {null, ""}) {
      Assertions.assertEquals(
          "ALTER TABLE \"native\"\"table\" ADD UNIQUE (\"quoted\"\"column\");",
          PostgreSqlTableOperations.addIndexDefinition(
              "native\"table",
              (TableChange.AddIndex)
                  TableChange.addIndex(
                      IndexType.UNIQUE_KEY, name, new String[][] {{"quoted\"column"}})));
    }
  }

  @Test
  void testUnnamedIndexesUsePostgreSqlGeneratedNames() {
    for (String name : new String[] {null, ""}) {
      Assertions.assertEquals(
          "ALTER TABLE \"t\" ADD UNIQUE (\"col1\");",
          PostgreSqlTableOperations.addIndexDefinition(
              "t",
              (TableChange.AddIndex)
                  TableChange.addIndex(IndexType.UNIQUE_KEY, name, new String[][] {{"col1"}})));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "column", "alterTable", "alterColumn", "addColumn"})
  void testNulCommentsAreRejectedBeforeDdl(String path) {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build();
    String bad = "before\0after";
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              switch (path) {
                case "table":
                  ops.createTableSql("t", new JdbcColumn[] {column}, bad);
                  break;
                case "column":
                  ops.createTableSql(
                      "t",
                      new JdbcColumn[] {
                        JdbcColumn.builder()
                            .withName("col1")
                            .withType(Types.StringType.get())
                            .withNullable(true)
                            .withComment(bad)
                            .build()
                      });
                  break;
                case "alterTable":
                  ops.alterTableSql("t", TableChange.updateComment(bad));
                  break;
                case "alterColumn":
                  ops.alterTableSql(
                      "t", TableChange.updateColumnComment(new String[] {"col1"}, bad));
                  break;
                case "addColumn":
                  ops.alterTableSql(
                      "t",
                      TableChange.addColumn(new String[] {"col2"}, Types.StringType.get(), bad));
                  break;
                default:
                  throw new AssertionError(path);
              }
            });
    Assertions.assertTrue(error.getMessage().contains("comment"));
    Assertions.assertTrue(error.getMessage().contains("U+0000"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"create", "add", "alter"})
  void testNulDefaultsAreRejectedOnEveryDdlPath(String path) {
    TestablePostgreSqlTableOperations ops =
        new TestablePostgreSqlTableOperations(new PostgreSqlColumnDefaultValueConverter());
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              switch (path) {
                case "create":
                  ops.createTableSql(
                      "t",
                      new JdbcColumn[] {
                        JdbcColumn.builder()
                            .withName("col1")
                            .withType(Types.StringType.get())
                            .withNullable(true)
                            .withDefaultValue(Literals.stringLiteral("bad\0default"))
                            .build()
                      });
                  break;
                case "add":
                  ops.alterTableSql(
                      "t",
                      TableChange.addColumn(
                          new String[] {"col2"},
                          Types.StringType.get(),
                          null,
                          TableChange.ColumnPosition.defaultPos(),
                          true,
                          false,
                          Literals.stringLiteral("bad\0default")));
                  break;
                case "alter":
                  ops.alterTableSql(
                      "t",
                      TableChange.updateColumnDefaultValue(
                          new String[] {"col1"}, Literals.stringLiteral("bad\0default")));
                  break;
                default:
                  throw new AssertionError(path);
              }
            });
    Assertions.assertTrue(error.getMessage().contains("default"));
    Assertions.assertTrue(error.getMessage().contains("U+0000"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDeleteIndexesDropsOnlyConstraints(boolean ifExists) {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();

    String sql =
        ops.alterTableSql(
            "test_table",
            TableChange.deleteIndex("primary_key", ifExists),
            TableChange.deleteIndex("unique_key", ifExists));

    String optional = ifExists ? "IF EXISTS " : "";
    Assertions.assertEquals(
        "ALTER TABLE \"test_table\" DROP CONSTRAINT "
            + optional
            + "\"primary_key\";\n"
            + "ALTER TABLE \"test_table\" DROP CONSTRAINT "
            + optional
            + "\"unique_key\";",
        sql);
  }

  @Test
  void testDeleteIndexWithoutIfExistsUsesStrictConstraintDrop() {
    TableUpdateRequest request =
        new TableUpdateRequest.DeleteTableIndexRequest("primary_key", null);
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();

    Assertions.assertEquals(
        "ALTER TABLE \"test_table\" DROP CONSTRAINT \"primary_key\";",
        ops.alterTableSql("test_table", request.tableChange()));
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
  public void testClearCommentsWithNull() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();

    Assertions.assertEquals(
        "COMMENT ON COLUMN \"test_table\".\"col1\" IS E'';",
        ops.alterTableSql(
            "test_table", TableChange.updateColumnComment(new String[] {"col1"}, null)));
    Assertions.assertEquals(
        "COMMENT ON TABLE \"test_table\" IS E'';",
        ops.alterTableSql("test_table", TableChange.updateComment(null)));
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

  @Test
  public void testMixedCaseTableNameIsQuotedOnAddColumn() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String alterSql =
        ops.alterTableSql(
            "MixedCaseTbl", TableChange.addColumn(new String[] {"extra"}, Types.IntegerType.get()));
    Assertions.assertTrue(alterSql.contains("ALTER TABLE \"MixedCaseTbl\""), alterSql);
  }

  @Test
  public void testMixedCaseTableNameIsQuotedOnUpdateColumnDefaultValue() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String alterSql =
        ops.alterTableSql(
            "MixedCaseTbl",
            TableChange.updateColumnDefaultValue(
                new String[] {"col1"}, Literals.of("abc", Types.VarCharType.of(255))));
    Assertions.assertTrue(alterSql.contains("ALTER TABLE \"MixedCaseTbl\""), alterSql);
  }

  @Test
  public void testMixedCaseTableNameIsQuotedOnUpdateColumnType() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String alterSql =
        ops.alterTableSql(
            "MixedCaseTbl",
            TableChange.updateColumnType(new String[] {"col1"}, Types.VarCharType.of(255)));
    Assertions.assertTrue(alterSql.contains("ALTER TABLE \"MixedCaseTbl\""), alterSql);
  }

  @Test
  public void testMixedCaseTableNameIsQuotedOnRenameColumn() {
    TestablePostgreSqlTableOperations ops = new TestablePostgreSqlTableOperations();
    String alterSql =
        ops.alterTableSql("MixedCaseTbl", TableChange.renameColumn(new String[] {"col1"}, "col2"));
    Assertions.assertTrue(alterSql.contains("ALTER TABLE \"MixedCaseTbl\""), alterSql);
  }
}
