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

import static org.apache.gravitino.catalog.clickhouse.ClickHouseUtils.getSortOrders;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
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

    Map<String, String> callGetTableProperties(Connection connection, String tableName)
        throws Exception {
      return getTableProperties(connection, tableName);
    }

    String callGenerateCreateTableSql(Map<String, String> properties) {
      JdbcColumn[] columns =
          new JdbcColumn[] {
            JdbcColumn.builder()
                .withName("id")
                .withType(Types.IntegerType.get())
                .withNullable(false)
                .build()
          };
      return generateCreateTableSql(
          "test_table",
          columns,
          "",
          properties,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES,
          getSortOrders("id"));
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

  private Map<String, String> loadTableProperties(String engine, String engineFull)
      throws Exception {
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    Mockito.when(resultSet.next()).thenReturn(true);
    Mockito.when(resultSet.getString("name")).thenReturn("test_table");
    Mockito.when(resultSet.getString("COMMENT")).thenReturn("");
    Mockito.when(resultSet.getString("ENGINE")).thenReturn(engine);
    Mockito.when(resultSet.getString("engine_full")).thenReturn(engineFull);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);

    Connection connection = Mockito.mock(Connection.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    return newOps().callGetTableProperties(connection, "test_table");
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
  // extractEngineParams
  // ---------------------------------------------------------------------------

  @Test
  void testExtractEngineParamsWithParams() {
    Assertions.assertEquals(
        "ts",
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree",
            "ReplacingMergeTree(ts) ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsMultipleParams() {
    Assertions.assertEquals(
        "sign, ts",
        ClickHouseTableOperations.extractEngineParams(
            "VersionedCollapsingMergeTree",
            "VersionedCollapsingMergeTree(sign, ts) ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsSingleParam() {
    Assertions.assertEquals(
        "sign",
        ClickHouseTableOperations.extractEngineParams(
            "CollapsingMergeTree",
            "CollapsingMergeTree(sign) ORDER BY id SETTINGS index_granularity = 8192"));
    Assertions.assertEquals(
        "val",
        ClickHouseTableOperations.extractEngineParams(
            "SummingMergeTree",
            "SummingMergeTree(val) ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsNoParams() {
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams(
            "MergeTree", "MergeTree ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsBlankInput() {
    Assertions.assertNull(ClickHouseTableOperations.extractEngineParams("MergeTree", null));
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams(null, "MergeTree ORDER BY id"));
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams("", "MergeTree ORDER BY id"));
  }

  @Test
  void testExtractEngineParamsEngineNameNotAtStart() {
    // The engine name must be at the start of engine_full.
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams("MergeTree", "something else MergeTree(x)"));
  }

  @Test
  void testExtractEngineParamsNestedParens() {
    // SummingMergeTree((a, b)) — nested parentheses should be preserved.
    Assertions.assertEquals(
        "(a, b)",
        ClickHouseTableOperations.extractEngineParams(
            "SummingMergeTree",
            "SummingMergeTree((a, b)) ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsGraphiteMergeTree() {
    // The generic scanner preserves the quoted parameter for Graphite-specific decoding.
    Assertions.assertEquals(
        "'graphite_rollup'",
        ClickHouseTableOperations.extractEngineParams(
            "GraphiteMergeTree",
            "GraphiteMergeTree('graphite_rollup') ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsAggregatingMergeTree() {
    // AggregatingMergeTree has no parameters.
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams(
            "AggregatingMergeTree",
            "AggregatingMergeTree ORDER BY id SETTINGS index_granularity = 8192"));
  }

  @Test
  void testExtractEngineParamsIgnoresParenthesesInsideQuotes() {
    Assertions.assertEquals(
        "`ver)`",
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "ReplacingMergeTree(`ver)`) ORDER BY id"));
    Assertions.assertEquals(
        "\"ver)\"",
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "ReplacingMergeTree(\"ver)\") ORDER BY id"));
    Assertions.assertEquals(
        "'rollup(test)'",
        ClickHouseTableOperations.extractEngineParams(
            "GraphiteMergeTree", "GraphiteMergeTree('rollup(test)') ORDER BY id"));
  }

  @Test
  void testExtractEngineParamsHandlesEscapedQuotes() {
    Assertions.assertEquals(
        "`ver``)`",
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "ReplacingMergeTree(`ver``)`) ORDER BY id"));
    Assertions.assertEquals(
        "'rollup\\')test'",
        ClickHouseTableOperations.extractEngineParams(
            "GraphiteMergeTree", "GraphiteMergeTree('rollup\\')test') ORDER BY id"));
  }

  @Test
  void testExtractEngineParamsAllowsWhitespaceBeforeParameters() {
    Assertions.assertEquals(
        "ts",
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "  ReplacingMergeTree \t (ts) ORDER BY id"));
  }

  @Test
  void testExtractEngineParamsRejectsUnclosedInput() {
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "ReplacingMergeTree('ver)) ORDER BY id"));
    Assertions.assertNull(
        ClickHouseTableOperations.extractEngineParams(
            "ReplacingMergeTree", "ReplacingMergeTree(tuple(ts) ORDER BY id"));
  }

  @Test
  void testGraphitePropertiesRoundTripThroughSqlGeneration() throws Exception {
    Map<String, String> loadedProperties =
        loadTableProperties(
            ENGINE.GRAPHITEMERGETREE.getValue(),
            "GraphiteMergeTree('graphite''rollup\\\\path') ORDER BY id");

    Assertions.assertEquals(
        "graphite'rollup\\path", loadedProperties.get(TableConstants.GRAPHITE_CONFIG));
    Assertions.assertFalse(loadedProperties.containsKey(TableConstants.ENGINE_PARAMETERS));

    String createSql = newOps().callGenerateCreateTableSql(loadedProperties);
    Assertions.assertTrue(
        createSql.contains("ENGINE = GraphiteMergeTree('graphite''rollup\\\\path')"), createSql);
  }

  @Test
  void testNonMergeTreeLoadDoesNotExposeEngineParameters() throws Exception {
    Map<String, String> loadedProperties =
        loadTableProperties(
            ENGINE.MySQL.getValue(), "MySQL('host:9000', 'database', 'table', 'user', 'secret')");

    Assertions.assertFalse(loadedProperties.containsKey(TableConstants.ENGINE_PARAMETERS));
    Assertions.assertFalse(
        loadedProperties.values().stream()
            .anyMatch(value -> value != null && value.contains("secret")));
  }

  @Test
  void testSupportedEngineParametersGenerateSql() {
    Map<String, String> properties = new HashMap<>();
    properties.put("engine", ENGINE.SUMMINGMERGETREE.getValue());
    properties.put(TableConstants.ENGINE_PARAMETERS, "(id)");

    String createSql = newOps().callGenerateCreateTableSql(properties);
    Assertions.assertTrue(createSql.contains("ENGINE = SummingMergeTree((id))"), createSql);
  }

  @Test
  void testUnsupportedEnginesRejectGenericParameters() {
    for (ENGINE engine :
        List.of(
            ENGINE.MERGETREE,
            ENGINE.AGGREGATINGMERGETREE,
            ENGINE.JOIN,
            ENGINE.MySQL,
            ENGINE.GRAPHITEMERGETREE,
            ENGINE.DISTRIBUTED)) {
      Map<String, String> properties = new HashMap<>();
      properties.put("engine", engine.getValue());
      properties.put(TableConstants.ENGINE_PARAMETERS, "sensitive_value");
      if (engine == ENGINE.GRAPHITEMERGETREE) {
        properties.put(TableConstants.GRAPHITE_CONFIG, "graphite_rollup");
      }

      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> newOps().callGenerateCreateTableSql(properties),
              engine.getValue());
      Assertions.assertTrue(exception.getMessage().contains("engine_parameters"));
      if (engine == ENGINE.GRAPHITEMERGETREE) {
        Assertions.assertTrue(exception.getMessage().contains("graphite.config"));
      }
    }
  }

  @Test
  void testSupportedEngineParametersCannotEscapeEngineClause() {
    Map<String, String> properties = new HashMap<>();
    properties.put("engine", ENGINE.REPLACINGMERGETREE.getValue());
    properties.put(TableConstants.ENGINE_PARAMETERS, "ts) SETTINGS index_granularity = 1");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> newOps().callGenerateCreateTableSql(properties));
    Assertions.assertTrue(exception.getMessage().contains("balanced"));
  @Test
  void testGetIndexesFailsOnMalformedParameterizedIndexMetadata() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();

    PreparedStatement primaryKeyStmt = Mockito.mock(PreparedStatement.class);
    ResultSet primaryKeyRs = Mockito.mock(ResultSet.class);
    PreparedStatement secondaryStmt = Mockito.mock(PreparedStatement.class);
    ResultSet secondaryRs = Mockito.mock(ResultSet.class);

    Mockito.when(primaryKeyRs.next()).thenReturn(false);
    Mockito.when(primaryKeyStmt.executeQuery()).thenReturn(primaryKeyRs);
    Mockito.when(secondaryRs.next()).thenReturn(true, false);
    Mockito.when(secondaryStmt.executeQuery()).thenReturn(secondaryRs);
    Mockito.when(secondaryRs.getString("name")).thenReturn("idx_bad");
    Mockito.when(secondaryRs.getString("type")).thenReturn("ngrambf_v1");
    Mockito.when(secondaryRs.getString("type_full")).thenReturn(null);
    Mockito.when(secondaryRs.getString("expr")).thenReturn("col_1");
    Mockito.when(secondaryRs.getLong("granularity")).thenReturn(1L);

    Connection connection = Mockito.mock(Connection.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
        .thenReturn(primaryKeyStmt)
        .thenReturn(secondaryStmt);

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> ops.callGetIndexes(connection, "db", "tbl"));
    Assertions.assertTrue(exception.getMessage().contains("idx_bad"));
    Assertions.assertTrue(exception.getMessage().contains("type_full"));
  }

  @Test
  void testGetIndexesFallsBackWhenTypeFullColumnIsMissing() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();

    PreparedStatement primaryKeyStmt = Mockito.mock(PreparedStatement.class);
    ResultSet primaryKeyRs = Mockito.mock(ResultSet.class);
    PreparedStatement modernSecondaryStmt = Mockito.mock(PreparedStatement.class);
    PreparedStatement legacySecondaryStmt = Mockito.mock(PreparedStatement.class);
    ResultSet legacySecondaryRs = Mockito.mock(ResultSet.class);

    Mockito.when(primaryKeyRs.next()).thenReturn(false);
    Mockito.when(primaryKeyStmt.executeQuery()).thenReturn(primaryKeyRs);
    Mockito.when(modernSecondaryStmt.executeQuery())
        .thenThrow(new SQLException("Unknown identifier 'type_full'"));
    Mockito.when(legacySecondaryStmt.executeQuery()).thenReturn(legacySecondaryRs);
    Mockito.when(legacySecondaryRs.next()).thenReturn(true, false);
    Mockito.when(legacySecondaryRs.getString("name")).thenReturn("idx_legacy");
    Mockito.when(legacySecondaryRs.getString("type")).thenReturn("ngrambf_v1(3, 512, 3, 0)");
    Mockito.when(legacySecondaryRs.getString("expr")).thenReturn("col_1");
    Mockito.when(legacySecondaryRs.getLong("granularity")).thenReturn(1L);

    Connection connection = Mockito.mock(Connection.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
        .thenReturn(primaryKeyStmt)
        .thenReturn(modernSecondaryStmt)
        .thenReturn(legacySecondaryStmt);

    List<Index> indexes = ops.callGetIndexes(connection, "db", "tbl");

    Assertions.assertEquals(1, indexes.size());
    Assertions.assertEquals(Index.IndexType.DATA_SKIPPING_NGRAMBFV1, indexes.get(0).type());
    Assertions.assertEquals(
        Map.of(
            "ngram_size", "3",
            "bloom_filter_size", "512",
            "hash_functions", "3",
            "random_seed", "0"),
        indexes.get(0).properties());
  }

  @Test
  void testGetIndexesDoesNotFallbackForOtherSqlErrors() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();

    PreparedStatement primaryKeyStmt = Mockito.mock(PreparedStatement.class);
    ResultSet primaryKeyRs = Mockito.mock(ResultSet.class);
    PreparedStatement secondaryStmt = Mockito.mock(PreparedStatement.class);
    Mockito.when(primaryKeyRs.next()).thenReturn(false);
    Mockito.when(primaryKeyStmt.executeQuery()).thenReturn(primaryKeyRs);
    Mockito.when(secondaryStmt.executeQuery())
        .thenThrow(new SQLException("Connection reset by peer"));

    Connection connection = Mockito.mock(Connection.class);
    Mockito.when(connection.prepareStatement(Mockito.anyString()))
        .thenReturn(primaryKeyStmt)
        .thenReturn(secondaryStmt);

    GravitinoRuntimeException exception =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> ops.callGetIndexes(connection, "db", "tbl"));
    Assertions.assertTrue(exception.getCause() instanceof SQLException);
    Mockito.verify(connection, Mockito.times(2)).prepareStatement(Mockito.anyString());
  }
}
