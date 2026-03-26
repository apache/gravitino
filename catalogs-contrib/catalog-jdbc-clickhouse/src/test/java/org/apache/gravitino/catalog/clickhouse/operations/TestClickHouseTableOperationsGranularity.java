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
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.DEFAULT_BLOOM_FILTER_GRANULARITY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.IndexConstants.GRANULARITY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseUtils.getSortOrders;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestClickHouseTableOperationsGranularity {

  private TestableClickHouseTableOperations newOps() {
    TestableClickHouseTableOperations ops = new TestableClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    return ops;
  }

  @Test
  void testGenerateCreateTableSqlGranularityDefaultAndConfigured() {
    TestableClickHouseTableOperations ops = newOps();
    JdbcColumn[] cols =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build(),
          JdbcColumn.builder()
              .withName("amount")
              .withType(Types.DoubleType.get())
              .withNullable(true)
              .build(),
          JdbcColumn.builder()
              .withName("note")
              .withType(Types.StringType.get())
              .withNullable(true)
              .build()
        };
    Index[] indexes =
        new Index[] {
          Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"id"}}),
          Indexes.of(
              Index.IndexType.DATA_SKIPPING_MINMAX,
              "idx_amount_mm",
              new String[][] {{"amount"}},
              Map.of(GRANULARITY, "9")),
          Indexes.of(
              Index.IndexType.DATA_SKIPPING_BLOOM_FILTER, "idx_note_bf", new String[][] {{"note"}})
        };

    String sql =
        ops.buildCreateSql(
            "t_granularity",
            cols,
            "comment",
            new HashMap<>(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            indexes,
            getSortOrders("id"));

    Assertions.assertTrue(sql.contains("INDEX `idx_amount_mm` `amount` TYPE minmax GRANULARITY 9"));
    Assertions.assertTrue(
        sql.contains(
            "INDEX `idx_note_bf` `note` TYPE bloom_filter GRANULARITY "
                + DEFAULT_BLOOM_FILTER_GRANULARITY));
  }

  @Test
  void testGenerateCreateTableSqlRejectInvalidGranularity() {
    TestableClickHouseTableOperations ops = newOps();
    JdbcColumn[] cols =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build(),
          JdbcColumn.builder()
              .withName("amount")
              .withType(Types.DoubleType.get())
              .withNullable(true)
              .build()
        };

    assertCreateGranularityInvalid(
        ops, cols, Map.of("GRANULARITY", "9"), "unsupported property key");
    assertCreateGranularityInvalid(
        ops, cols, Map.of(GRANULARITY, " "), "It must be a positive integer.");
    assertCreateGranularityInvalid(
        ops, cols, Map.of(GRANULARITY, "abc"), "It must be a positive integer.");
    assertCreateGranularityInvalid(
        ops, cols, Map.of(GRANULARITY, "0"), "It must be a positive integer.");
    assertCreateGranularityInvalid(
        ops, cols, Map.of(GRANULARITY, "-1"), "It must be a positive integer.");
  }

  @Test
  void testGenerateAlterTableSqlRejectInvalidGranularity() {
    TestableClickHouseTableOperations ops = newOps();
    ops.setTable(buildStubTable());

    assertAlterGranularityInvalid(ops, Map.of("GRANULARITY", "9"), "unsupported property key");
    assertAlterGranularityInvalid(ops, Map.of(GRANULARITY, " "), "It must be a positive integer.");
    assertAlterGranularityInvalid(
        ops, Map.of(GRANULARITY, "abc"), "It must be a positive integer.");
    assertAlterGranularityInvalid(ops, Map.of(GRANULARITY, "0"), "It must be a positive integer.");
    assertAlterGranularityInvalid(ops, Map.of(GRANULARITY, "-1"), "It must be a positive integer.");
  }

  @Test
  void testGenerateAlterTableSqlGranularityDefaultAndConfigured() {
    TestableClickHouseTableOperations ops = newOps();
    ops.setTable(buildStubTable());

    String minmaxSql =
        ops.buildAlterSql(
            "db",
            "tbl",
            new TableChange[] {
              TableChange.addIndex(
                  Index.IndexType.DATA_SKIPPING_MINMAX,
                  "idx_amount_mm",
                  new String[][] {{"amount"}},
                  Map.of(GRANULARITY, "11"))
            });
    Assertions.assertTrue(minmaxSql.contains("ADD INDEX `idx_amount_mm` `amount` TYPE minmax"));
    Assertions.assertTrue(minmaxSql.contains("GRANULARITY 11"));

    String bloomSql =
        ops.buildAlterSql(
            "db",
            "tbl",
            new TableChange[] {
              TableChange.addIndex(
                  Index.IndexType.DATA_SKIPPING_BLOOM_FILTER,
                  "idx_note_bf",
                  new String[][] {{"note"}})
            });
    Assertions.assertTrue(
        bloomSql.contains(
            "ADD INDEX `idx_note_bf` `note` TYPE bloom_filter GRANULARITY "
                + DEFAULT_BLOOM_FILTER_GRANULARITY));
  }

  @Test
  void testGetSecondaryIndexesSkipsUnsupportedRowsAndLoadsValidGranularity() throws Exception {
    TestableClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

    // Case 1: valid minmax index with explicit granularity.
    // Case 2: valid bloom_filter index with blank granularity (no properties key).
    // Case 3: invalid expression, should be skipped.
    // Case 4: unsupported index type, should be skipped.
    Mockito.when(resultSet.next()).thenReturn(true, true, true, true, false);
    Mockito.when(resultSet.getString("name"))
        .thenReturn("idx_valid_mm", "idx_valid_bf", "idx_invalid_expr", "idx_invalid_type");
    Mockito.when(resultSet.getString("type"))
        .thenReturn("minmax", "bloom_filter", "minmax", "unknown_type");
    Mockito.when(resultSet.getString("expr"))
        .thenReturn("`amount`", "`note`", "amount + 1", "`note`");
    Mockito.when(resultSet.getString("granularity")).thenReturn("9", "", "7", "5");

    List<Index> indexes = ops.invokeGetSecondaryIndexes(connection, "db", "tbl");

    // Only Case 1 and Case 2 should be loaded.
    Assertions.assertEquals(2, indexes.size());

    // Case 1 verification: granularity is loaded into index properties.
    Assertions.assertEquals("idx_valid_mm", indexes.get(0).name());
    Assertions.assertEquals(Index.IndexType.DATA_SKIPPING_MINMAX, indexes.get(0).type());
    Assertions.assertEquals("9", indexes.get(0).properties().get(GRANULARITY));

    // Case 2 verification: blank granularity does not create a granularity property.
    Assertions.assertEquals("idx_valid_bf", indexes.get(1).name());
    Assertions.assertEquals(Index.IndexType.DATA_SKIPPING_BLOOM_FILTER, indexes.get(1).type());
    Assertions.assertFalse(indexes.get(1).properties().containsKey(GRANULARITY));
  }

  private void assertCreateGranularityInvalid(
      TestableClickHouseTableOperations ops,
      JdbcColumn[] cols,
      Map<String, String> indexProperties,
      String expectedMessage) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.buildCreateSql(
                    "t_granularity_invalid",
                    cols,
                    "comment",
                    new HashMap<>(),
                    Transforms.EMPTY_TRANSFORM,
                    Distributions.NONE,
                    new Index[] {
                      Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"id"}}),
                      Indexes.of(
                          Index.IndexType.DATA_SKIPPING_MINMAX,
                          "idx_amount_mm",
                          new String[][] {{"amount"}},
                          indexProperties)
                    },
                    getSortOrders("id")));
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
  }

  private void assertAlterGranularityInvalid(
      TestableClickHouseTableOperations ops,
      Map<String, String> indexProperties,
      String expectedMessage) {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                ops.buildAlterSql(
                    "db",
                    "tbl",
                    new TableChange[] {
                      TableChange.addIndex(
                          Index.IndexType.DATA_SKIPPING_MINMAX,
                          "idx_amount_mm",
                          new String[][] {{"amount"}},
                          indexProperties)
                    }));
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
  }

  private static JdbcTable buildStubTable() {
    JdbcColumn id =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .withDefaultValue(DEFAULT_VALUE_NOT_SET)
            .build();
    JdbcColumn amount =
        JdbcColumn.builder()
            .withName("amount")
            .withType(Types.DoubleType.get())
            .withNullable(true)
            .build();
    JdbcColumn note =
        JdbcColumn.builder()
            .withName("note")
            .withType(Types.StringType.get())
            .withNullable(true)
            .build();
    return JdbcTable.builder()
        .withName("tbl")
        .withColumns(new JdbcColumn[] {id, amount, note})
        .withIndexes(
            new Index[] {
              Indexes.primary(Indexes.DEFAULT_PRIMARY_KEY_NAME, new String[][] {{"id"}})
            })
        .withComment("table_comment")
        .withTableOperation(null)
        .build();
  }

  private static final class TestableClickHouseTableOperations extends ClickHouseTableOperations {
    private JdbcTable table;

    String buildCreateSql(
        String tableName,
        JdbcColumn[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitioning,
        org.apache.gravitino.rel.expressions.distributions.Distribution distribution,
        Index[] indexes,
        SortOrder[] sortOrders) {
      return generateCreateTableSql(
          tableName, columns, comment, properties, partitioning, distribution, indexes, sortOrders);
    }

    String buildAlterSql(String db, String tableName, TableChange[] changes) {
      return generateAlterTableSql(db, tableName, changes);
    }

    void setTable(JdbcTable table) {
      this.table = table;
    }

    @SuppressWarnings("unchecked")
    List<Index> invokeGetSecondaryIndexes(Connection connection, String db, String tableName)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method =
          ClickHouseTableOperations.class.getDeclaredMethod(
              "getSecondaryIndexes", Connection.class, String.class, String.class);
      method.setAccessible(true);
      return (List<Index>) method.invoke(this, connection, db, tableName);
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      return table;
    }
  }
}
