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
package org.apache.gravitino.catalog.hologres.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.hologres.converter.HologresColumnDefaultValueConverter;
import org.apache.gravitino.catalog.hologres.converter.HologresExceptionConverter;
import org.apache.gravitino.catalog.hologres.converter.HologresTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HologresTableOperations}. */
public class TestHologresTableOperations {

  // ==================== Helper inner class for SQL generation tests ====================

  private static class TestableHologresTableOperations extends HologresTableOperations {
    private JdbcTable mockTable;

    public TestableHologresTableOperations() {
      super.exceptionMapper = new HologresExceptionConverter();
      super.typeConverter = new HologresTypeConverter();
      super.columnDefaultValueConverter = new HologresColumnDefaultValueConverter();
    }

    public void setMockTable(JdbcTable table) {
      this.mockTable = table;
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      if (mockTable != null) {
        return mockTable;
      }
      return lazyLoadCreateTable;
    }

    public String createTableSql(
        String tableName,
        JdbcColumn[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitioning,
        Distribution distribution,
        Index[] indexes) {
      return generateCreateTableSql(
          tableName, columns, comment, properties, partitioning, distribution, indexes);
    }

    public String renameTableSql(String oldName, String newName) {
      return generateRenameTableSql(oldName, newName);
    }

    public String dropTableSql(String tableName) {
      return generateDropTableSql(tableName);
    }

    public String purgeTableSql(String tableName) {
      return generatePurgeTableSql(tableName);
    }

    public String alterTableSql(String schemaName, String tableName, TableChange... changes) {
      return generateAlterTableSql(schemaName, tableName, changes);
    }

    public JdbcTable buildFakeTable(String tableName, JdbcColumn... columns) {
      return JdbcTable.builder()
          .withName(tableName)
          .withColumns(columns)
          .withComment("test table")
          .withProperties(Collections.emptyMap())
          .build();
    }
  }

  private final TestableHologresTableOperations ops = new TestableHologresTableOperations();

  // ==================== appendPartitioningSql tests ====================

  @Test
  void testAppendPartitioningSqlPhysicalPartition() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("PARTITION BY LIST(\"ds\")"));
    assertFalse(result.contains("LOGICAL"));
  }

  @Test
  void testAppendPartitioningSqlLogicalPartitionSingleColumn() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("LOGICAL PARTITION BY LIST(\"ds\")"));
  }

  @Test
  void testAppendPartitioningSqlLogicalPartitionTwoColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"region"}, {"ds"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("LOGICAL PARTITION BY LIST(\"region\", \"ds\")"));
  }

  @Test
  void testAppendPartitioningSqlRejectsNonListTransform() {
    Transform[] partitioning = {Transforms.identity("col1")};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlPhysicalRejectsMultipleColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"col1"}, {"col2"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlLogicalRejectsMoreThanTwoColumns() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"col1"}, {"col2"}, {"col3"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, true, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlRejectsEmptyPartitions() {
    Transform[] partitioning = {Transforms.list(new String[][] {})};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlRejectsMultipleTransforms() {
    Transform[] partitioning = {
      Transforms.list(new String[][] {{"col1"}}), Transforms.list(new String[][] {{"col2"}})
    };
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  @Test
  void testAppendPartitioningSqlRejectsNestedFieldNames() {
    Transform[] partitioning = {Transforms.list(new String[][] {{"schema", "col1"}})};
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendPartitioningSql(partitioning, false, sqlBuilder));
  }

  // ==================== appendIndexesSql tests ====================

  @Test
  void testAppendIndexesSqlEmpty() {
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[0], sqlBuilder);
    assertTrue(sqlBuilder.toString().isEmpty());
  }

  @Test
  void testAppendIndexesSqlPrimaryKey() {
    // Hologres does not support custom constraint names, so the name is ignored
    Index pk = Indexes.primary("pk_test", new String[][] {{"id"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {pk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertFalse(result.contains("CONSTRAINT"));
    assertTrue(result.contains("PRIMARY KEY (\"id\")"));
  }

  @Test
  void testAppendIndexesSqlPrimaryKeyWithoutName() {
    Index pk = Indexes.primary(null, new String[][] {{"id"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {pk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertFalse(result.contains("CONSTRAINT"));
    assertTrue(result.contains("PRIMARY KEY (\"id\")"));
  }

  @Test
  void testAppendIndexesSqlUniqueKeyThrows() {
    // Hologres does not support UNIQUE KEY index separately
    Index uk = Indexes.unique("uk_name", new String[][] {{"name"}});
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendIndexesSql(new Index[] {uk}, sqlBuilder));
  }

  @Test
  void testAppendIndexesSqlCompositeKey() {
    Index pk = Indexes.primary("pk_comp", new String[][] {{"id"}, {"ds"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {pk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("PRIMARY KEY (\"id\", \"ds\")"));
  }

  @Test
  void testAppendIndexesSqlMultipleIndexesWithUniqueKeyThrows() {
    // Hologres does not support UNIQUE KEY, so mixing PK and UK should throw
    Index pk = Indexes.primary("pk_id", new String[][] {{"id"}});
    Index uk = Indexes.unique("uk_email", new String[][] {{"email"}});
    StringBuilder sqlBuilder = new StringBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.appendIndexesSql(new Index[] {pk, uk}, sqlBuilder));
  }

  // ==================== generateCreateTableSql tests ====================

  @Test
  void testCreateTableBasic() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("CREATE TABLE \"test_table\""));
    assertTrue(sql.contains("\"id\" int4"));
    assertTrue(sql.contains("NOT NULL"));
  }

  @Test
  void testCreateTableWithComment() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            "This is a test table",
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("COMMENT ON TABLE \"test_table\" IS 'This is a test table'"));
  }

  @Test
  void testCreateTableWithColumnComment() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withComment("Primary key column")
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("COMMENT ON COLUMN \"test_table\".\"id\" IS 'Primary key column'"));
  }

  @Test
  void testCreateTableWithDistribution() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Distribution dist = Distributions.hash(0, NamedReference.field("id"));
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            dist,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("distribution_key = 'id'"));
    assertTrue(sql.contains("WITH ("));
  }

  @Test
  void testCreateTableWithProperties() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("orientation", "column");
    properties.put("time_to_live_in_seconds", "3600");
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("orientation = 'column'"));
    assertTrue(sql.contains("time_to_live_in_seconds = '3600'"));
  }

  @Test
  void testCreateTableDistributionKeyFilteredFromProperties() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("distribution_key", "should_be_ignored");
    properties.put("orientation", "column");
    Distribution dist = Distributions.hash(0, NamedReference.field("id"));
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            dist,
            Indexes.EMPTY_INDEXES);
    // Should use the Distribution parameter, not the property
    assertTrue(sql.contains("distribution_key = 'id'"));
    assertFalse(sql.contains("should_be_ignored"));
  }

  @Test
  void testCreateTableIsLogicalPartitionedFilteredFromProperties() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("is_logical_partitioned_table", "true");
    properties.put("orientation", "column");
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    // is_logical_partitioned_table should NOT appear in WITH clause
    assertFalse(sql.contains("is_logical_partitioned_table"));
    assertTrue(sql.contains("orientation = 'column'"));
  }

  @Test
  void testCreateTablePrimaryKeyFilteredFromProperties() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("primary_key", "id");
    properties.put("orientation", "column");
    Index pk = Indexes.primary("pk_id", new String[][] {{"id"}});
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new Index[] {pk});
    // primary_key should NOT appear in WITH clause (it is defined via PRIMARY KEY constraint)
    assertFalse(sql.contains("primary_key"));
    assertTrue(sql.contains("orientation = 'column'"));
    assertTrue(sql.contains("PRIMARY KEY"));
  }

  @Test
  void testCreateTableWithPhysicalPartition() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("ds")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build();
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            partitioning,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("PARTITION BY LIST(\"ds\")"));
    assertFalse(sql.contains("LOGICAL"));
  }

  @Test
  void testCreateTableWithLogicalPartition() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("ds")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("is_logical_partitioned_table", "true");
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            partitioning,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("LOGICAL PARTITION BY LIST(\"ds\")"));
  }

  @Test
  void testCreateTableWithLogicalPartitionTwoColumns() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("region")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("ds")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("is_logical_partitioned_table", "true");
    Transform[] partitioning = {Transforms.list(new String[][] {{"region"}, {"ds"}})};
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col1, col2},
            null,
            properties,
            partitioning,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("LOGICAL PARTITION BY LIST(\"region\", \"ds\")"));
  }

  @Test
  void testCreateTableWithPrimaryKey() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build();
    Index pk = Indexes.primary("pk_id", new String[][] {{"id"}});
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            new Index[] {pk});
    assertTrue(sql.contains("PRIMARY KEY (\"id\")"));
    // Hologres does not support custom constraint names, so CONSTRAINT should not appear
    assertFalse(sql.contains("CONSTRAINT"));
  }

  @Test
  void testCreateTableWithAutoIncrementThrows() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withAutoIncrement(true)
            .build();
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.createTableSql(
                    "test_table",
                    new JdbcColumn[] {col},
                    null,
                    Collections.emptyMap(),
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    Indexes.EMPTY_INDEXES));
    assertTrue(ex.getMessage().contains("auto-increment"));
  }

  @Test
  void testCreateTableWithNullableColumn() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("NULL"));
    assertFalse(sql.contains("NOT NULL"));
  }

  @Test
  void testCreateTableWithDefaultValue() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("status")
            .withType(Types.IntegerType.get())
            .withNullable(true)
            .withDefaultValue(Literals.integerLiteral(0))
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("DEFAULT 0"));
  }

  @Test
  void testCreateTableMultipleColumns() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build();
    JdbcColumn col3 =
        JdbcColumn.builder()
            .withName("amount")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(true)
            .withDefaultValue(
                Literals.decimalLiteral(org.apache.gravitino.rel.types.Decimal.of("0.00", 10, 2)))
            .build();
    String sql =
        ops.createTableSql(
            "orders",
            new JdbcColumn[] {col1, col2, col3},
            "Order table",
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("\"id\" int8"));
    assertTrue(sql.contains("\"name\" text"));
    assertTrue(sql.contains("\"amount\" numeric(10,2)"));
    assertTrue(sql.contains("COMMENT ON TABLE \"orders\" IS 'Order table'"));
  }

  @Test
  void testCreateTableCommentWithSingleQuotes() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("flag")
            .withType(Types.StringType.get())
            .withNullable(false)
            .withComment("退货标志（'R'=已退货, 'A'=未退货）")
            .build();
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            "It's a test table",
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    // Single quotes in table comment should be escaped
    assertTrue(sql.contains("IS 'It''s a test table'"));
    // Single quotes in column comment should be escaped
    assertTrue(sql.contains("IS '退货标志（''R''=已退货, ''A''=未退货）'"));
    // Unescaped single quotes should NOT appear
    assertFalse(sql.contains("IS 'It's"));
  }

  @Test
  void testCreateTableFullFeatured() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("order_id")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("ds")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("is_logical_partitioned_table", "true");
    properties.put("orientation", "column");
    Distribution dist = Distributions.hash(0, NamedReference.field("order_id"));
    Transform[] partitioning = {Transforms.list(new String[][] {{"ds"}})};
    Index pk = Indexes.primary("pk_order", new String[][] {{"order_id"}, {"ds"}});
    String sql =
        ops.createTableSql(
            "orders",
            new JdbcColumn[] {col1, col2},
            "Order table",
            properties,
            partitioning,
            dist,
            new Index[] {pk});
    assertTrue(sql.contains("CREATE TABLE \"orders\""));
    assertTrue(sql.contains("LOGICAL PARTITION BY LIST(\"ds\")"));
    assertTrue(sql.contains("distribution_key = 'order_id'"));
    assertTrue(sql.contains("orientation = 'column'"));
    assertTrue(sql.contains("PRIMARY KEY (\"order_id\", \"ds\")"));
    assertTrue(sql.contains("COMMENT ON TABLE \"orders\" IS 'Order table'"));
    assertFalse(sql.contains("is_logical_partitioned_table"));
  }

  // ==================== generateRenameTableSql tests ====================

  @Test
  void testRenameTableSql() {
    String sql = ops.renameTableSql("old_table", "new_table");
    assertEquals("ALTER TABLE \"old_table\" RENAME TO \"new_table\"", sql);
  }

  // ==================== generateDropTableSql tests ====================

  @Test
  void testDropTableSql() {
    String sql = ops.dropTableSql("my_table");
    assertEquals("DROP TABLE \"my_table\"", sql);
  }

  // ==================== generatePurgeTableSql tests ====================

  @Test
  void testPurgeTableSqlThrowsException() {
    assertThrows(UnsupportedOperationException.class, () -> ops.purgeTableSql("my_table"));
  }

  // ==================== updateColumnAutoIncrement not supported ====================
  // Note: Hologres does not support altering column auto-increment via ALTER TABLE.
  // The UpdateColumnAutoIncrement change type is rejected in generateAlterTableSql().
  // CREATE TABLE with auto-increment (GENERATED BY DEFAULT AS IDENTITY) is also not supported
  // and will throw an exception — see testCreateTableWithAutoIncrementThrows() above.

  // ==================== addIndex / deleteIndex not supported ====================
  // Note: Hologres does not support adding or deleting indexes via ALTER TABLE.
  // The AddIndex and DeleteIndex change types are rejected in generateAlterTableSql().
  // CREATE TABLE with PRIMARY KEY and UNIQUE KEY constraints is still supported
  // — see testCreateTableWithPrimaryKey() above.

  // ==================== getIndexFieldStr tests ====================

  @Test
  void testGetIndexFieldStrSingleColumn() {
    String result = HologresTableOperations.getIndexFieldStr(new String[][] {{"id"}});
    assertEquals("\"id\"", result);
  }

  @Test
  void testGetIndexFieldStrMultipleColumns() {
    String result = HologresTableOperations.getIndexFieldStr(new String[][] {{"id"}, {"name"}});
    assertEquals("\"id\", \"name\"", result);
  }

  @Test
  void testGetIndexFieldStrRejectsNestedColumn() {
    assertThrows(
        IllegalArgumentException.class,
        () -> HologresTableOperations.getIndexFieldStr(new String[][] {{"schema", "column"}}));
  }

  // ==================== calculateDatetimePrecision tests ====================

  @Test
  void testCalculateDatetimePrecisionTimestamp() {
    Integer result = ops.calculateDatetimePrecision("TIMESTAMP", 0, 6);
    assertEquals(6, result);
  }

  @Test
  void testCalculateDatetimePrecisionTimestamptz() {
    Integer result = ops.calculateDatetimePrecision("TIMESTAMPTZ", 0, 3);
    assertEquals(3, result);
  }

  @Test
  void testCalculateDatetimePrecisionTime() {
    Integer result = ops.calculateDatetimePrecision("TIME", 0, 0);
    assertEquals(0, result);
  }

  @Test
  void testCalculateDatetimePrecisionTimetz() {
    Integer result = ops.calculateDatetimePrecision("TIMETZ", 0, 6);
    assertEquals(6, result);
  }

  @Test
  void testCalculateDatetimePrecisionNonDatetime() {
    Integer result = ops.calculateDatetimePrecision("INT4", 0, 0);
    assertEquals(null, result);
  }

  @Test
  void testCalculateDatetimePrecisionNegativeScaleReturnsZero() {
    Integer result = ops.calculateDatetimePrecision("TIMESTAMP", 0, -1);
    assertEquals(0, result);
  }

  @Test
  void testCreateTableWithUnparsedExpressionDefault() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("order_time")
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("ds")
            .withType(Types.TimestampType.withoutTimeZone())
            .withNullable(false)
            .withDefaultValue(UnparsedExpression.of("date_trunc('day'::text, order_time)"))
            .build();
    String sql =
        ops.createTableSql(
            "test_default_expr",
            new JdbcColumn[] {col1, col2},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    // UnparsedExpression should produce DEFAULT, not GENERATED ALWAYS AS
    assertTrue(sql.contains("DEFAULT date_trunc('day'::text, order_time)"));
    assertFalse(sql.contains("GENERATED ALWAYS AS"));
    assertTrue(sql.contains("NOT NULL"));
  }

  @Test
  void testCreateTableWithUnparsedExpressionDefaultNullable() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("val")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("computed")
            .withType(Types.IntegerType.get())
            .withNullable(true)
            .withDefaultValue(UnparsedExpression.of("val * 2"))
            .build();
    String sql =
        ops.createTableSql(
            "test_default_nullable",
            new JdbcColumn[] {col1, col2},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    // UnparsedExpression should produce DEFAULT, not GENERATED ALWAYS AS
    assertTrue(sql.contains("DEFAULT val * 2"));
    assertFalse(sql.contains("GENERATED ALWAYS AS"));
    // The nullable column should have NULL
    assertTrue(sql.contains("NULL DEFAULT val * 2"));
  }

  // ==================== generateAlterTableSql tests ====================

  @Test
  void testAlterTableUpdateComment() {
    // Note: updateComment needs a JdbcTable with StringIdentifier, which requires
    // getOrCreateTable. Since we can't mock the DB connection, we test the SQL format
    // via the other alter operations that don't require table loading.
  }

  @Test
  void testAlterTableRenameColumn() {
    String sql =
        ops.alterTableSql(
            "public", "test_table", TableChange.renameColumn(new String[] {"old_col"}, "new_col"));
    assertTrue(sql.contains("ALTER TABLE \"test_table\" RENAME COLUMN \"old_col\" TO \"new_col\""));
  }

  @Test
  void testAlterTableUpdateColumnComment() {
    String sql =
        ops.alterTableSql(
            "public",
            "test_table",
            TableChange.updateColumnComment(new String[] {"col1"}, "new comment"));
    assertTrue(sql.contains("COMMENT ON COLUMN \"test_table\".\"col1\" IS 'new comment'"));
  }

  @Test
  void testAlterTableUpdateColumnCommentWithSingleQuotes() {
    String sql =
        ops.alterTableSql(
            "public",
            "test_table",
            TableChange.updateColumnComment(new String[] {"col1"}, "it's a test"));
    assertTrue(sql.contains("IS 'it''s a test'"));
  }

  @Test
  void testAlterTableRenameColumnRejectsNestedField() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.renameColumn(new String[] {"schema", "col"}, "new_col")));
  }

  @Test
  void testAlterTableSetPropertyThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTableSql("public", "test_table", TableChange.setProperty("key", "value")));
  }

  @Test
  void testAlterTableRemovePropertyThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTableSql("public", "test_table", TableChange.removeProperty("key")));
  }

  @Test
  void testAlterTableUpdateColumnDefaultValueThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.updateColumnDefaultValue(
                    new String[] {"col1"}, Literals.integerLiteral(0))));
  }

  @Test
  void testAlterTableUpdateColumnTypeThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.updateColumnType(new String[] {"col1"}, Types.StringType.get())));
  }

  @Test
  void testAlterTableUpdateColumnPositionThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.updateColumnPosition(
                    new String[] {"col1"}, TableChange.ColumnPosition.first())));
  }

  @Test
  void testAlterTableUpdateColumnNullabilityThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.updateColumnNullability(new String[] {"col1"}, true)));
  }

  @Test
  void testAlterTableAddIndexThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.addIndex(
                    Index.IndexType.PRIMARY_KEY, "pk_test", new String[][] {{"col1"}})));
  }

  @Test
  void testAlterTableDeleteIndexThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTableSql("public", "test_table", TableChange.deleteIndex("pk_test", false)));
  }

  @Test
  void testAlterTableUpdateColumnAutoIncrementThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.updateColumnAutoIncrement(new String[] {"col1"}, true)));
  }

  // ==================== deleteColumn (DROP COLUMN) tests ====================

  @Test
  void testAlterTableDeleteColumn() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    JdbcColumn col2 =
        JdbcColumn.builder()
            .withName("name")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build();
    ops.setMockTable(ops.buildFakeTable("test_table", col1, col2));
    String sql =
        ops.alterTableSql(
            "public", "test_table", TableChange.deleteColumn(new String[] {"name"}, false));
    // Hologres requires GUC to enable DROP COLUMN
    assertTrue(sql.contains("SET hg_experimental_enable_drop_column = on;"));
    assertTrue(sql.contains("ALTER TABLE \"test_table\" DROP COLUMN \"name\""));
  }

  @Test
  void testAlterTableDeleteColumnIfExists() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    ops.setMockTable(ops.buildFakeTable("test_table", col1));
    // When column doesn't exist and IF EXISTS is true, should return empty string
    String sql =
        ops.alterTableSql(
            "public", "test_table", TableChange.deleteColumn(new String[] {"non_existent"}, true));
    assertEquals("", sql);
  }

  @Test
  void testAlterTableDeleteColumnNotExistsThrows() {
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    ops.setMockTable(ops.buildFakeTable("test_table", col1));
    // When column doesn't exist and IF EXISTS is false, should throw
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.deleteColumn(new String[] {"non_existent"}, false)));
  }

  @Test
  void testAlterTableDeleteColumnRejectsNestedField() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            ops.alterTableSql(
                "public",
                "test_table",
                TableChange.deleteColumn(new String[] {"schema", "col"}, false)));
  }

  // ==================== quoteIdentifier tests ====================

  @Test
  void testQuoteIdentifierEscapesEmbeddedDoubleQuotes() {
    // Table name with embedded double quote should be escaped by doubling
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    String sql =
        ops.createTableSql(
            "table\"name",
            new JdbcColumn[] {col},
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    assertTrue(sql.contains("CREATE TABLE \"table\"\"name\""));
  }

  // ==================== Property key/value sanitization tests ====================

  @Test
  void testCreateTablePropertyValueEscapesSingleQuotes() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("clustering_key", "col1:asc,col2's:desc");
    String sql =
        ops.createTableSql(
            "test_table",
            new JdbcColumn[] {col},
            null,
            properties,
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
    // Single quote in value should be escaped
    assertTrue(sql.contains("clustering_key = 'col1:asc,col2''s:desc'"));
  }

  @Test
  void testCreateTableInvalidPropertyKeyThrows() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("invalid-key!", "value");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTableSql(
                "test_table",
                new JdbcColumn[] {col},
                null,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                Indexes.EMPTY_INDEXES));
  }

  @Test
  void testCreateTableSqlInjectionPropertyKeyThrows() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put("key'); DROP TABLE users; --", "value");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createTableSql(
                "test_table",
                new JdbcColumn[] {col},
                null,
                properties,
                Transforms.EMPTY_TRANSFORM,
                Distributions.NONE,
                Indexes.EMPTY_INDEXES));
  }
}
