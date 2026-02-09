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
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
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
    public TestableHologresTableOperations() {
      super.exceptionMapper = new HologresExceptionConverter();
      super.typeConverter = new HologresTypeConverter();
      super.columnDefaultValueConverter = new HologresColumnDefaultValueConverter();
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
    Index pk = Indexes.primary("pk_test", new String[][] {{"id"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {pk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("CONSTRAINT \"pk_test\""));
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
  void testAppendIndexesSqlUniqueKey() {
    Index uk = Indexes.unique("uk_name", new String[][] {{"name"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {uk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("CONSTRAINT \"uk_name\""));
    assertTrue(result.contains("UNIQUE (\"name\")"));
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
  void testAppendIndexesSqlMultipleIndexes() {
    Index pk = Indexes.primary("pk_id", new String[][] {{"id"}});
    Index uk = Indexes.unique("uk_email", new String[][] {{"email"}});
    StringBuilder sqlBuilder = new StringBuilder();
    HologresTableOperations.appendIndexesSql(new Index[] {pk, uk}, sqlBuilder);
    String result = sqlBuilder.toString();
    assertTrue(result.contains("PRIMARY KEY"));
    assertTrue(result.contains("UNIQUE"));
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
    assertTrue(sql.contains("CONSTRAINT \"pk_id\""));
  }

  @Test
  void testCreateTableWithAutoIncrement() {
    JdbcColumn col =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withAutoIncrement(true)
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
    assertTrue(sql.contains("GENERATED BY DEFAULT AS IDENTITY"));
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

  // ==================== updateColumnAutoIncrementDefinition tests ====================

  @Test
  void testUpdateColumnAutoIncrementAdd() {
    TableChange.UpdateColumnAutoIncrement change =
        (TableChange.UpdateColumnAutoIncrement)
            TableChange.updateColumnAutoIncrement(new String[] {"id"}, true);
    String sql = HologresTableOperations.updateColumnAutoIncrementDefinition(change, "test_table");
    assertTrue(sql.contains("ALTER TABLE \"test_table\""));
    assertTrue(sql.contains("ALTER COLUMN"));
    assertTrue(sql.contains("\"id\""));
    assertTrue(sql.contains("ADD GENERATED BY DEFAULT AS IDENTITY"));
  }

  @Test
  void testUpdateColumnAutoIncrementDrop() {
    TableChange.UpdateColumnAutoIncrement change =
        (TableChange.UpdateColumnAutoIncrement)
            TableChange.updateColumnAutoIncrement(new String[] {"id"}, false);
    String sql = HologresTableOperations.updateColumnAutoIncrementDefinition(change, "test_table");
    assertTrue(sql.contains("DROP IDENTITY"));
  }

  @Test
  void testUpdateColumnAutoIncrementRejectsNestedColumn() {
    TableChange.UpdateColumnAutoIncrement change =
        (TableChange.UpdateColumnAutoIncrement)
            TableChange.updateColumnAutoIncrement(new String[] {"struct", "field"}, true);
    assertThrows(
        UnsupportedOperationException.class,
        () -> HologresTableOperations.updateColumnAutoIncrementDefinition(change, "test_table"));
  }

  // ==================== addIndexDefinition tests ====================

  @Test
  void testAddIndexDefinitionPrimaryKey() {
    TableChange.AddIndex addIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "pk_id", new String[][] {{"id"}});
    String sql = HologresTableOperations.addIndexDefinition("test_table", addIndex);
    assertTrue(sql.contains("ALTER TABLE \"test_table\""));
    assertTrue(sql.contains("ADD CONSTRAINT \"pk_id\""));
    assertTrue(sql.contains("PRIMARY KEY"));
    assertTrue(sql.contains("(\"id\")"));
  }

  @Test
  void testAddIndexDefinitionUniqueKey() {
    TableChange.AddIndex addIndex =
        (TableChange.AddIndex)
            TableChange.addIndex(
                Index.IndexType.UNIQUE_KEY, "uk_email", new String[][] {{"email"}});
    String sql = HologresTableOperations.addIndexDefinition("test_table", addIndex);
    assertTrue(sql.contains("UNIQUE"));
    assertTrue(sql.contains("\"uk_email\""));
  }

  // ==================== deleteIndexDefinition tests ====================

  @Test
  void testDeleteIndexDefinition() {
    TableChange.DeleteIndex deleteIndex =
        (TableChange.DeleteIndex) TableChange.deleteIndex("pk_id", false);
    String sql = HologresTableOperations.deleteIndexDefinition("test_table", deleteIndex);
    assertTrue(sql.contains("ALTER TABLE \"test_table\" DROP CONSTRAINT \"pk_id\""));
    assertTrue(sql.contains("DROP INDEX \"pk_id\""));
    assertFalse(sql.contains("IF EXISTS"));
  }

  @Test
  void testDeleteIndexDefinitionIfExists() {
    TableChange.DeleteIndex deleteIndex =
        (TableChange.DeleteIndex) TableChange.deleteIndex("pk_id", true);
    String sql = HologresTableOperations.deleteIndexDefinition("test_table", deleteIndex);
    assertTrue(sql.contains("DROP INDEX IF EXISTS \"pk_id\""));
  }

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
}
