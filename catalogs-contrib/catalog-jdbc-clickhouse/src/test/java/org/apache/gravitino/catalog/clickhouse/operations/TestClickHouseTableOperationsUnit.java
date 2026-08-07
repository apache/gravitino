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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestClickHouseTableOperationsUnit {

  private static final class ExposedClickHouseTableOperations extends ClickHouseTableOperations {
    List<Index> callGetIndexes(Connection connection, String databaseName, String tableName)
        throws Exception {
      return getIndexes(connection, databaseName, tableName);
    }

    String callStripProjections(String createSql) {
      return stripProjections(createSql);
    }

    SortOrder[] callParseSortOrdersFromCreateSql(String createSql) {
      return parseSortOrdersFromCreateSql(createSql);
    }
  }

  private ExposedClickHouseTableOperations newOps() {
    ExposedClickHouseTableOperations ops = new ExposedClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    return ops;
  }

  // ---------------------------------------------------------------------------
  // getIndexes — SQL injection escape
  // ---------------------------------------------------------------------------

  @Test
  void testGetIndexesSqlEscapesSingleQuotes() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();

    PreparedStatement primaryKeyStmt = Mockito.mock(PreparedStatement.class);
    ResultSet primaryKeyRs = Mockito.mock(ResultSet.class);
    PreparedStatement secondaryStmt = Mockito.mock(PreparedStatement.class);
    ResultSet secondaryRs = Mockito.mock(ResultSet.class);

    Mockito.when(primaryKeyRs.next()).thenReturn(false);
    Mockito.when(primaryKeyStmt.executeQuery()).thenReturn(primaryKeyRs);
    Mockito.when(secondaryRs.next()).thenReturn(false);
    Mockito.when(secondaryStmt.executeQuery()).thenReturn(secondaryRs);

    Connection connection = Mockito.mock(Connection.class);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.when(connection.prepareStatement(sqlCaptor.capture()))
        .thenReturn(primaryKeyStmt)
        .thenReturn(secondaryStmt);

    ops.callGetIndexes(connection, "db'1", "t'1");

    // First captured SQL is the primary-key QUERY_INDEXES_SQL (string-interpolated).
    String primaryKeySql = sqlCaptor.getAllValues().get(0);
    Assertions.assertTrue(
        primaryKeySql.contains("db''1"), "database single quote should be doubled");
    Assertions.assertTrue(primaryKeySql.contains("t''1"), "table single quote should be doubled");
  }

  // ---------------------------------------------------------------------------
  // stripProjections — unit tests
  // ---------------------------------------------------------------------------

  @Test
  void testStripProjectionsNormalWithOrderBy() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.t1\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_normal\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    // The PROJECTION block must be removed
    Assertions.assertFalse(stripped.contains("PROJECTION"), "PROJECTION keyword should be removed");
    Assertions.assertFalse(stripped.contains("SELECT *"), "projection body should be removed");
    // The table-level ORDER BY must remain
    Assertions.assertTrue(
        stripped.contains("ORDER BY (id, dt)"), "table-level ORDER BY must be preserved");
    // Engine and SETTINGS must remain
    Assertions.assertTrue(stripped.contains("ENGINE = MergeTree"));
    Assertions.assertTrue(stripped.contains("SETTINGS index_granularity = 8192"));
  }

  @Test
  void testStripProjectionsAggregateOnly() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.t6\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_agg\n"
            + "    (\n"
            + "        SELECT\n"
            + "            dt,\n"
            + "            count() AS cnt\n"
            + "        GROUP BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"), "PROJECTION keyword should be removed");
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsMultiple() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.t2\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_normal\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    ),\n"
            + "    PROJECTION p_agg\n"
            + "    (\n"
            + "        SELECT\n"
            + "            dt,\n"
            + "            count() AS cnt\n"
            + "        GROUP BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsNoProjection() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.normal\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    // Must be unchanged (no PROJECTION to strip)
    Assertions.assertEquals(ddl, stripped);
  }

  @Test
  void testStripProjectionsWithIndex() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.t3\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    INDEX idx_val val TYPE minmax GRANULARITY 1,\n"
            + "    PROJECTION p_normal\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    // INDEX must be preserved
    Assertions.assertTrue(stripped.contains("INDEX idx_val val TYPE minmax GRANULARITY 1"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsFirstPosition() {
    ExposedClickHouseTableOperations ops = newOps();
    // PROJECTION as the first element in the body
    String ddl =
        "CREATE TABLE test_proj.first\n"
            + "(\n"
            + "    PROJECTION p_first\n"
            + "    (\n"
            + "        SELECT id, dt ORDER BY id\n"
            + "    ),\n"
            + "    `id` Int64,\n"
            + "    `dt` Date\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("`id` Int64"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsMidPosition() {
    ExposedClickHouseTableOperations ops = newOps();
    // PROJECTION between columns
    String ddl =
        "CREATE TABLE test_proj.mid\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    PROJECTION p_mid\n"
            + "    (\n"
            + "        SELECT * ORDER BY dt\n"
            + "    ),\n"
            + "    `dt` Date,\n"
            + "    `val` String\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("`id` Int64"));
    Assertions.assertTrue(stripped.contains("`dt` Date"));
    Assertions.assertTrue(stripped.contains("`val` String"));
  }

  @Test
  void testStripProjectionsEmptyDdl() {
    ExposedClickHouseTableOperations ops = newOps();
    Assertions.assertEquals("", ops.callStripProjections(""));
    Assertions.assertNull(ops.callStripProjections(null));
  }

  @Test
  void testStripProjectionsBacktickQuotedName() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.bt\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    PROJECTION `p_backtick`\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsWithDescAndComplexBody() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.desc\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    PROJECTION p_desc\n"
            + "    (\n"
            + "        SELECT * ORDER BY dt DESC, id\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  @Test
  void testStripProjectionsStringLiteralContainsProjection() {
    // Column default value containing the word "PROJECTION" must not trigger
    // a false match — the string-literal skipping must be applied first.
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.strlit\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `desc` String DEFAULT 'PROJECTION is not a keyword here',\n"
            + "    `dt` Date\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    // No projection exists, so the DDL must be preserved verbatim.
    Assertions.assertEquals(ddl, stripped);
    Assertions.assertTrue(stripped.contains("PROJECTION is not a keyword here"));
  }

  @Test
  void testStripProjectionsProjectionBodyWithStringLiteral() {
    // Projection body containing an escaped string literal ('').
    // The inner string-skipping logic must handle '' correctly.
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.body\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    PROJECTION p_body\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        WHERE dt > 'it''s a test'\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    String stripped = ops.callStripProjections(ddl);
    Assertions.assertFalse(stripped.contains("PROJECTION"));
    Assertions.assertTrue(stripped.contains("ORDER BY (id, dt)"));
  }

  // ---------------------------------------------------------------------------
  // parseSortOrdersFromCreateSql — unit tests (with PROJECTION)
  // ---------------------------------------------------------------------------

  @Test
  void testParseSortOrdersNormalProjection() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.t1\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_normal\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertTrue(sortOrders[0].expression() instanceof NamedReference);
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertTrue(sortOrders[1].expression() instanceof NamedReference);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersAggregateProjection() {
    ExposedClickHouseTableOperations ops = newOps();
    // Aggregate projection has GROUP BY, no ORDER BY — table-level ORDER BY should be parsed
    String ddl =
        "CREATE TABLE test_proj.agg\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_agg\n"
            + "    (\n"
            + "        SELECT\n"
            + "            dt,\n"
            + "            count() AS cnt\n"
            + "        GROUP BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersMultipleProjections() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.multi\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p1\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    ),\n"
            + "    PROJECTION p2\n"
            + "    (\n"
            + "        SELECT id, val\n"
            + "        ORDER BY id DESC\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(
        2, sortOrders.length, "Should extract table-level sort keys, not projection-internal ones");
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersNoProjectionNoRegression() {
    ExposedClickHouseTableOperations ops = newOps();
    String ddl =
        "CREATE TABLE test_proj.normal\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersProjectionWithPrimaryKeyAndPartition() {
    ExposedClickHouseTableOperations ops = newOps();
    // Complex DDL: PARTITION BY + PRIMARY KEY + projection + ORDER BY
    String ddl =
        "CREATE TABLE test_proj.t5\n"
            + "(\n"
            + "    `id` Int64,\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_normal\n"
            + "    (\n"
            + "        SELECT *\n"
            + "        ORDER BY dt\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "PARTITION BY toYYYYMM(dt)\n"
            + "PRIMARY KEY (id, dt)\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersProjectionFirstInColumns() {
    ExposedClickHouseTableOperations ops = newOps();
    // PROJECTION defined before any column
    String ddl =
        "CREATE TABLE test_proj.first\n"
            + "(\n"
            + "    PROJECTION p_first\n"
            + "    (\n"
            + "        SELECT id, dt ORDER BY id\n"
            + "    ),\n"
            + "    `id` Int64,\n"
            + "    `dt` Date\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }

  @Test
  void testParseSortOrdersUnionAllProjectionsCovered() {
    ExposedClickHouseTableOperations ops = newOps();
    // Three projections at different positions: first, middle, last
    String ddl =
        "CREATE TABLE test_proj.three\n"
            + "(\n"
            + "    PROJECTION p_first\n"
            + "    (\n"
            + "        SELECT id, dt ORDER BY id\n"
            + "    ),\n"
            + "    `id` Int64,\n"
            + "    PROJECTION p_mid\n"
            + "    (\n"
            + "        SELECT * ORDER BY dt\n"
            + "    ),\n"
            + "    `dt` Date,\n"
            + "    `val` String,\n"
            + "    PROJECTION p_last\n"
            + "    (\n"
            + "        SELECT * ORDER BY val\n"
            + "    )\n"
            + ")\n"
            + "ENGINE = MergeTree\n"
            + "ORDER BY (id, dt)\n"
            + "SETTINGS index_granularity = 8192";

    SortOrder[] sortOrders = ops.callParseSortOrdersFromCreateSql(ddl);
    Assertions.assertEquals(
        2, sortOrders.length, "All three projections must be stripped; table ORDER BY parsed");
    Assertions.assertEquals("id", ((NamedReference) sortOrders[0].expression()).fieldName()[0]);
    Assertions.assertEquals("dt", ((NamedReference) sortOrders[1].expression()).fieldName()[0]);
  }
}
