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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksTableOperations;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.mockito.Mockito;

public class TestStarRocksTableOperationsSqlGeneration {

  private static class TestableStarRocksTableOperations extends StarRocksTableOperations {

    private final String[] columnNames;

    public TestableStarRocksTableOperations() {
      this(new String[0]);
    }

    private TestableStarRocksTableOperations(String... columnNames) {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new StarRocksTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
      this.columnNames = columnNames;
    }

    private TestableStarRocksTableOperations(
        ConnectionCountingDataSource connections, String... columnNames) {
      this(columnNames);
      dataSource = connections.dataSource();
    }

    public String createTableSql(
        String tableName, JdbcColumn[] columns, Distribution distribution) {
      return createTableSql(tableName, columns, distribution, "comment");
    }

    public String createTableSql(
        String tableName, JdbcColumn[] columns, Distribution distribution, String comment) {
      return createTableSql(tableName, columns, distribution, comment, Indexes.EMPTY_INDEXES);
    }

    String createTableSql(
        String tableName,
        JdbcColumn[] columns,
        Distribution distribution,
        String comment,
        Index[] indexes) {
      return generateCreateTableSql(
          tableName,
          columns,
          comment,
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          distribution,
          indexes);
    }

    public String alterTableSql(String tableName, TableChange... changes) {
      return generateAlterTableSql("database", tableName, changes);
    }

    List<Index> indexes(Connection connection, String databaseName, String tableName)
        throws SQLException {
      return getIndexes(connection, databaseName, tableName);
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      return JdbcTable.builder()
          .withName(tableName)
          .withColumns(
              Arrays.stream(columnNames)
                  .map(
                      name ->
                          JdbcColumn.builder()
                              .withName(name)
                              .withType(Types.IntegerType.get())
                              .build())
                  .toArray(JdbcColumn[]::new))
          .withComment(
              StringIdentifier.addToComment(StringIdentifier.fromId(42), "existing comment"))
          .build();
    }
  }

  @ParameterizedTest
  @EnumSource(Index.IndexType.class)
  void testCreateTableRejectsIndexesBeforeConnecting(Index.IndexType indexType) {
    TestableStarRocksTableOperations ops =
        new TestableStarRocksTableOperations() {
          @Override
          protected Connection getConnection(String databaseName) {
            throw new AssertionError("Index validation must happen before acquiring a connection");
          }
        };
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(Types.IntegerType.get()).build();
    Index index = Indexes.of(indexType, "test_index", new String[][] {{"col1"}});

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                ops.create(
                    "database",
                    "test_table",
                    new JdbcColumn[] {column},
                    "comment",
                    Collections.emptyMap(),
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.hash(1, NamedReference.field("col1")),
                    new Index[] {index}));

    Assertions.assertEquals(
        "StarRocks catalog does not support creating table indexes. "
            + "Remove indexes from the create table request.",
        exception.getMessage());
  }

  @ParameterizedTest
  @NullSource
  @EmptySource
  void testCreateTableWithoutIndexes(Index[] indexes) {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(Types.IntegerType.get()).build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    Assertions.assertEquals(
        ops.createTableSql("test_table", new JdbcColumn[] {column}, distribution),
        ops.createTableSql(
            "test_table", new JdbcColumn[] {column}, distribution, "comment", indexes));
  }

  @Test
  void testPurgeIsRejectedWithoutExecutingSql() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations(connections);

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class, () -> ops.purge("database", "test_table"));

    Assertions.assertEquals(
        "StarRocks does not support purge table in Gravitino, please use drop table",
        exception.getMessage());
    Assertions.assertTrue(connections.executedSql().isEmpty());
    Assertions.assertEquals(0, connections.borrowed());
  }

  @Test
  void testDropStillExecutesDropTable() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations(connections);

    Assertions.assertTrue(ops.drop("database", "test_table"));

    Assertions.assertEquals(List.of("DROP TABLE `test_table`"), connections.executedSql());
    Assertions.assertEquals(0, connections.borrowed());
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

    // The existing table's identifier (uid42) is preserved, and a sacrificial DUMMY_ID (uid-1)
    // marker is appended on top: the read path (extractTableCommentFromSql) strips exactly one
    // (outermost) marker, so the real identifier survives the round-trip.
    String alterSql = ops.alterTableSql("test_table", TableChange.updateComment(tableComment));
    Assertions.assertTrue(
        alterSql.contains(
            "COMMENT = \"owner\"\"; DROP TABLE marker; -- "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid42) "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid-1)\""),
        alterSql);

    String clearCommentSql = ops.alterTableSql("test_table", TableChange.updateComment(""));
    Assertions.assertTrue(
        clearCommentSql.contains(
            "COMMENT = \"(From Gravitino, DO NOT EDIT: gravitino.v1.uid42) "
                + "(From Gravitino, DO NOT EDIT: gravitino.v1.uid-1)\""),
        clearCommentSql);
    Assertions.assertEquals(
        clearCommentSql, ops.alterTableSql("test_table", TableChange.updateComment(null)));
  }

  @Test
  public void testClearTableCommentWithNull() {
    TestableStarRocksTableOperations ops =
        new TestableStarRocksTableOperations() {
          @Override
          protected JdbcTable getOrCreateTable(
              String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
            return JdbcTable.builder().withName(tableName).build();
          }
        };

    String alterSql = ops.alterTableSql("test_table", TableChange.updateComment(null));
    Assertions.assertTrue(alterSql.contains("COMMENT = \"\""), alterSql);
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

  @Test
  void testSetPropertyEscapesKeyAndValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    String sql =
        ops.alterTableSql(
            "test_table",
            TableChange.setProperty(
                "dynamic_partition.key\"name", "owner's \"comment\" C:\\tmp\"); --"));
    Assertions.assertEquals(
        "ALTER TABLE `test_table`\n"
            + "set ( \"dynamic_partition.key\"\"name\" = \"owner's \"\"comment\"\" C:\\\\tmp\"\"); --\" );",
        sql);
  }

  @Test
  public void testAddColumnWithNotNullAndDefaultValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(
                new String[] {"created_at"},
                Types.TimestampType.withoutTimeZone(),
                "create time",
                TableChange.ColumnPosition.after("id"),
                false,
                false,
                Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP));

    Assertions.assertTrue(
        alterSql.contains(
            "ADD COLUMN `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP "
                + "COMMENT 'create time' AFTER `id`"),
        alterSql);
  }

  @Test
  public void testAddNullableColumnWithStringDefaultValue() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(
                new String[] {"col2"},
                Types.VarCharType.of(10),
                null,
                TableChange.ColumnPosition.after("col1"),
                true,
                false,
                Literals.of("abc", Types.VarCharType.of(10))));

    Assertions.assertTrue(
        alterSql.contains("ADD COLUMN `col2` varchar(10) NULL DEFAULT 'abc' AFTER `col1`"),
        alterSql);
  }

  @Test
  public void testAddAutoIncrementColumnIsRejected() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.alterTableSql(
                    "test_table",
                    TableChange.addColumn(
                        new String[] {"col2"},
                        Types.LongType.get(),
                        null,
                        TableChange.ColumnPosition.defaultPos(),
                        false,
                        true)));

    Assertions.assertEquals(
        "StarRocks does not support adding an auto-increment column: col2", exception.getMessage());
  }

  @Test
  void testGetIndexesMapsTypesAndSkipsUnsupported() throws SQLException {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(connection.prepareStatement("SHOW INDEX FROM `table` FROM `database`"))
        .thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true, true, true, true, true, true, false);
    Mockito.when(resultSet.getString("Key_name"))
        .thenReturn("idx_region", "idx_ngram", "idx_text", "idx_vector", "idx_unknown", "idx_null");
    Mockito.when(resultSet.getString("Column_name"))
        .thenReturn("region", "name", "text", "embedding", "c5", "c6");
    Mockito.when(resultSet.getString("Index_type"))
        .thenReturn(
            "BITMAP",
            "NGRAMBF(\"gram_num\" = \"4\")",
            "GIN(\"parser\" = \"english\")",
            "VECTOR(\"index_type\" = \"hnsw\", \"dim\" = \"3\")",
            "UNKNOWN",
            null);

    List<Index> indexes = ops.indexes(connection, "database", "table");

    Assertions.assertEquals(3, indexes.size());
    Assertions.assertEquals(Index.IndexType.BITMAP, indexes.get(0).type());
    Assertions.assertEquals("idx_region", indexes.get(0).name());
    Assertions.assertArrayEquals(new String[][] {{"region"}}, indexes.get(0).fieldNames());
    Assertions.assertEquals(Index.IndexType.INVERTED, indexes.get(1).type());
    Assertions.assertEquals("idx_text", indexes.get(1).name());
    Assertions.assertArrayEquals(new String[][] {{"text"}}, indexes.get(1).fieldNames());
    Assertions.assertEquals(Index.IndexType.VECTOR, indexes.get(2).type());
    Assertions.assertEquals("idx_vector", indexes.get(2).name());
    Assertions.assertArrayEquals(new String[][] {{"embedding"}}, indexes.get(2).fieldNames());
  }

  @Test
  void testRenameColumnQuotesIdentifiers() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations("id", "order-id");

    String alterSql =
        ops.alterTableSql("test_table", TableChange.renameColumn(new String[] {"order-id"}, "key"));

    Assertions.assertEquals(
        "ALTER TABLE `test_table`\nRENAME COLUMN `order-id` TO `key`;", alterSql);
  }

  @Test
  void testDeleteMissingColumnIfExistsSkipsAlter() {
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    TestableStarRocksTableOperations ops =
        new TestableStarRocksTableOperations(connections, "id", "col1");
    TableChange deleteMissing = TableChange.deleteColumn(new String[] {"missing"}, true);

    Assertions.assertEquals("", ops.alterTableSql("test_table", deleteMissing));

    ops.alterTable("database", "test_table", deleteMissing);
    Assertions.assertTrue(connections.executedSql().isEmpty());
  }

  @Test
  void testDeleteMissingColumnIfExistsWithOtherChanges() {
    TestableStarRocksTableOperations ops = new TestableStarRocksTableOperations("id", "col1");

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.deleteColumn(new String[] {"missing"}, true),
            TableChange.deleteColumn(new String[] {"col1"}, false));

    Assertions.assertEquals("ALTER TABLE `test_table`\nDROP COLUMN `col1`;", alterSql);
  }
}
