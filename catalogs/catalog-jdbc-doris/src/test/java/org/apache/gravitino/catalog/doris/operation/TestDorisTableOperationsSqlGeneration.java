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
package org.apache.gravitino.catalog.doris.operation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
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
import org.mockito.Mockito;

public class TestDorisTableOperationsSqlGeneration {

  private static class TestableDorisTableOperations extends DorisTableOperations {
    public TestableDorisTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new DorisTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
      try {
        // Set up a mock DataSource for validateAutoIncrementVersion
        // Uses SHOW FRONTENDS to get the actual Doris version (not MySQL protocol version)
        DataSource mockDataSource = Mockito.mock(DataSource.class);
        Connection mockConnection = Mockito.mock(Connection.class);
        Statement mockStatement = Mockito.mock(Statement.class);
        ResultSet mockResultSet = Mockito.mock(ResultSet.class);
        ResultSetMetaData mockMetaData = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(mockDataSource.getConnection()).thenReturn(mockConnection);
        Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);
        Mockito.when(mockStatement.executeQuery("SHOW FRONTENDS")).thenReturn(mockResultSet);
        Mockito.when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
        Mockito.when(mockMetaData.getColumnCount()).thenReturn(1);
        Mockito.when(mockMetaData.getColumnLabel(1)).thenReturn("Version");
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getString(1)).thenReturn("doris-3.0.6.2-rc01-910c4249c5");
        super.dataSource = mockDataSource;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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

    public String createTableSqlWithIndexes(
        String tableName, JdbcColumn[] columns, Distribution distribution, Index[] indexes) {
      return generateCreateTableSql(
          tableName,
          columns,
          "comment",
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          distribution,
          indexes);
    }
  }

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withDefaultValue(Literals.of("", Types.VarCharType.of(255)))
            .build();
    // Doris requires distribution
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql = mockOps.createTableSql(tableName, new JdbcColumn[] {col1}, distribution);
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT '' but was: " + sql);
  }

  @Test
  public void testCreateTableWithNonEmptyStringDefaultValue() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withDefaultValue(Literals.of("abc", Types.VarCharType.of(255)))
            .build();
    // Doris requires distribution
    Distribution distribution = Distributions.hash(1, NamedReference.field("col1"));

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql = mockOps.createTableSql(tableName, new JdbcColumn[] {col1}, distribution);
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT value but was: " + sql);
  }

  @Test
  public void testEscapeCommentsInGeneratedSql() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());
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
        mockOps.createTableSql("test_table", new JdbcColumn[] {column}, distribution, tableComment);
    Assertions.assertTrue(
        createSql.contains("COMMENT \"owner\"\"; DROP TABLE marker; --\""), createSql);
    Assertions.assertTrue(
        createSql.contains("COMMENT 'owner''s comment; DROP TABLE marker; --'"), createSql);

    String alterSql =
        mockOps.alterTableSql(
            "test_table", TableChange.updateColumnComment(new String[] {"col1"}, columnComment));
    Assertions.assertTrue(
        alterSql.contains("MODIFY COLUMN `col1` COMMENT 'owner''s comment; DROP TABLE marker; --'"),
        alterSql);
  }

  @Test
  public void testEscapeAddColumnCommentInGeneratedSql() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    String comment = "owner\\'s \"comment\"; --";

    String alterSql =
        ops.alterTableSql(
            "test_table",
            TableChange.addColumn(new String[] {"col2"}, Types.IntegerType.get(), comment));

    Assertions.assertTrue(alterSql.contains("ADD COLUMN `col2`"), alterSql);
    Assertions.assertTrue(alterSql.contains("COMMENT 'owner\\\\''s \"comment\"; --'"), alterSql);
  }

  @Test
  public void testCreateTableWithPrimaryKeyIndex() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    JdbcColumn idCol =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn nameCol =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.VarCharType.of(100))
            .withNullable(true)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("id"));

    // PRIMARY_KEY stays in the INDEX clause (not filtered) for backward compatibility
    // with Doris 1.2.x. No USING clause — Doris defaults to the appropriate index type.
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.PRIMARY_KEY, "PRIMARY", new String[][] {{"id"}})};

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql =
        mockOps.createTableSqlWithIndexes(
            "test_pk", new JdbcColumn[] {idCol, nameCol}, distribution, indexes);
    Assertions.assertTrue(
        sql.contains("INDEX `PRIMARY` (`id`)"), "PRIMARY_KEY should be in INDEX clause: " + sql);
    Assertions.assertFalse(sql.contains("USING"), "No USING clause for PRIMARY_KEY: " + sql);
    Assertions.assertFalse(
        sql.contains("UNIQUE KEY"), "PRIMARY_KEY should not generate UNIQUE KEY: " + sql);
  }

  @Test
  public void testCreateTableWithInvertedIndex() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    JdbcColumn idCol =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn nameCol =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.VarCharType.of(100))
            .withNullable(true)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("id"));

    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.INVERTED, "idx_name", new String[][] {{"name"}})};

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql =
        mockOps.createTableSqlWithIndexes(
            "test_inverted", new JdbcColumn[] {idCol, nameCol}, distribution, indexes);
    Assertions.assertTrue(
        sql.contains("INDEX `idx_name` (`name`) USING INVERTED"),
        "Should generate INVERTED index: " + sql);
  }

  @Test
  public void testCreateTableWithBitmapIndex() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    JdbcColumn idCol =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn tagCol =
        JdbcColumn.builder()
            .withName("tag")
            .withType(Types.IntegerType.get())
            .withNullable(true)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("id"));

    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.BITMAP, "idx_tag", new String[][] {{"tag"}})};

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql =
        mockOps.createTableSqlWithIndexes(
            "test_bitmap", new JdbcColumn[] {idCol, tagCol}, distribution, indexes);
    Assertions.assertTrue(
        sql.contains("INDEX `idx_tag` (`tag`)") && !sql.contains("INDEX `idx_tag` (`tag`) USING"),
        "BITMAP index should omit USING clause for cross-version compatibility: " + sql);
  }

  @Test
  public void testMapDorisIndexType() {
    Assertions.assertEquals(
        Index.IndexType.PRIMARY_KEY, DorisTableOperations.mapDorisIndexType("BTREE", "PRIMARY"));
    Assertions.assertEquals(
        Index.IndexType.UNIQUE_KEY, DorisTableOperations.mapDorisIndexType("BTREE", "uk_col1"));
    Assertions.assertEquals(
        Index.IndexType.INVERTED, DorisTableOperations.mapDorisIndexType("INVERTED", "idx_name"));
    // BITMAP mapped to INVERTED for cross-version compatibility (Doris 4.0.6 removed BITMAP
    // from Nereids grammar)
    Assertions.assertEquals(
        Index.IndexType.INVERTED, DorisTableOperations.mapDorisIndexType("BITMAP", "idx_name"));
    Assertions.assertEquals(
        Index.IndexType.DATA_SKIPPING_BLOOM_FILTER,
        DorisTableOperations.mapDorisIndexType("BLOOMFILTER", "idx_name"));
    Assertions.assertEquals(
        Index.IndexType.VECTOR, DorisTableOperations.mapDorisIndexType("ANN", "idx_name"));
    // Unknown type should fall back to INVERTED
    Assertions.assertEquals(
        Index.IndexType.INVERTED,
        DorisTableOperations.mapDorisIndexType("UNKNOWN_TYPE", "idx_name"));
    // Null index type (Doris 1.2.x without Index_type column):
    // PRIMARY index name → PRIMARY_KEY
    Assertions.assertEquals(
        Index.IndexType.PRIMARY_KEY, DorisTableOperations.mapDorisIndexType(null, "PRIMARY"));
    // Non-primary index name → UNIQUE_KEY (Doris 1.2.x indexes are all BTREE-based key indexes)
    Assertions.assertEquals(
        Index.IndexType.UNIQUE_KEY, DorisTableOperations.mapDorisIndexType(null, "idx_name"));
  }

  @Test
  public void testCreateTableWithAutoIncrement() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    JdbcColumn idCol =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.LongType.get())
            .withNullable(false)
            .withAutoIncrement(true)
            .build();
    JdbcColumn nameCol =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.VarCharType.of(100))
            .withNullable(true)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("id"));

    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.PRIMARY_KEY, "PRIMARY", new String[][] {{"id"}})};

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql =
        mockOps.createTableSqlWithIndexes(
            "test_auto_incr", new JdbcColumn[] {idCol, nameCol}, distribution, indexes);
    Assertions.assertTrue(sql.contains("AUTO_INCREMENT"), "Should contain AUTO_INCREMENT: " + sql);
    // PRIMARY_KEY stays in INDEX clause for backward compatibility with Doris 1.2.x
    Assertions.assertTrue(
        sql.contains("INDEX `PRIMARY` (`id`)"), "PRIMARY_KEY should be in INDEX clause: " + sql);
    Assertions.assertFalse(
        sql.contains("UNIQUE KEY"), "PRIMARY_KEY should not generate UNIQUE KEY: " + sql);
  }

  @Test
  public void testCreateTableWithUniqueKeyIndex() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    JdbcColumn idCol =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build();
    JdbcColumn nameCol =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.VarCharType.of(100))
            .withNullable(true)
            .build();
    Distribution distribution = Distributions.hash(1, NamedReference.field("id"));

    // UNIQUE_KEY index explicitly generates UNIQUE KEY declaration (Doris 2.0+)
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.UNIQUE_KEY, "uk_id", new String[][] {{"id"}})};

    TestableDorisTableOperations mockOps = Mockito.spy(ops);
    Mockito.doAnswer(a -> a.getArgument(0))
        .when(mockOps)
        .appendNecessaryProperties(Mockito.anyMap());

    String sql =
        mockOps.createTableSqlWithIndexes(
            "test_uk", new JdbcColumn[] {idCol, nameCol}, distribution, indexes);
    Assertions.assertTrue(
        sql.contains("UNIQUE KEY(`id`)"), "UNIQUE_KEY should generate UNIQUE KEY: " + sql);
    Assertions.assertFalse(
        sql.contains("INDEX `"), "UNIQUE_KEY should not appear in INDEX clause: " + sql);
  }

  @Test
  public void testAddIndexDefinition() {
    // INVERTED index
    TableChange.AddIndex addIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.INVERTED, "idx_name", new String[][] {{"col1"}});
    String sql = DorisTableOperations.addIndexDefinition(addIndex);
    Assertions.assertEquals("ADD INDEX `idx_name` (`col1`) USING INVERTED", sql);

    // BITMAP index — omits USING clause for cross-version compatibility
    addIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.BITMAP, "idx_tag", new String[][] {{"tag"}});
    sql = DorisTableOperations.addIndexDefinition(addIndex);
    Assertions.assertEquals("ADD INDEX `idx_tag` (`tag`)", sql);

    // VECTOR index (maps to ANN)
    addIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.VECTOR, "idx_vec", new String[][] {{"embedding"}});
    sql = DorisTableOperations.addIndexDefinition(addIndex);
    Assertions.assertEquals("ADD INDEX `idx_vec` (`embedding`) USING ANN", sql);
  }

  @Test
  public void testAddPrimaryKeyIndexDefinitionThrows() {
    // PRIMARY_KEY cannot be added via ALTER TABLE ADD INDEX in Doris
    TableChange.AddIndex primaryKeyIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "PRIMARY", new String[][] {{"id"}});
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> DorisTableOperations.addIndexDefinition(primaryKeyIndex));

    // UNIQUE_KEY cannot be added via ALTER TABLE ADD INDEX in Doris
    TableChange.AddIndex uniqueKeyIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.UNIQUE_KEY, "uk_id", new String[][] {{"id"}});
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> DorisTableOperations.addIndexDefinition(uniqueKeyIndex));
  }

  @Test
  public void testDeleteIndexDefinition() {
    // deleteIndexDefinition should quote the index name with backticks, matching addIndexDefinition
    JdbcTable mockTable =
        JdbcTable.builder()
            .withName("t")
            .withColumns(new org.apache.gravitino.catalog.jdbc.JdbcColumn[0])
            .withIndexes(
                new Index[] {
                  Indexes.of(Index.IndexType.INVERTED, "idx_name", new String[][] {{"col1"}})
                })
            .build();
    TableChange.DeleteIndex deleteIndex =
        (TableChange.DeleteIndex) TableChange.deleteIndex("idx_name", true);
    String sql = DorisTableOperations.deleteIndexDefinition(mockTable, deleteIndex);
    Assertions.assertEquals("DROP INDEX `idx_name`", sql);
  }

  @Test
  public void testIsVersionAtLeast() {
    // Exact match
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("2.1.0", 2, 1, 0));
    // Higher version
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("3.0.6.2", 2, 1, 0));
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("4.0.6", 2, 1, 0));
    // Lower version
    Assertions.assertFalse(DorisTableOperations.isVersionAtLeast("1.2.7", 2, 1, 0));
    Assertions.assertFalse(DorisTableOperations.isVersionAtLeast("2.0.0", 2, 1, 0));
    // With suffix
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("2.1.0-rc01", 2, 1, 0));
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("3.0.6.2-merged", 2, 1, 0));
    // Null/empty
    Assertions.assertFalse(DorisTableOperations.isVersionAtLeast(null, 2, 1, 0));
    Assertions.assertFalse(DorisTableOperations.isVersionAtLeast("", 2, 1, 0));
    // Patch level comparison
    Assertions.assertTrue(DorisTableOperations.isVersionAtLeast("2.1.1", 2, 1, 0));
    Assertions.assertFalse(DorisTableOperations.isVersionAtLeast("2.1.0", 2, 1, 1));
  }
}
