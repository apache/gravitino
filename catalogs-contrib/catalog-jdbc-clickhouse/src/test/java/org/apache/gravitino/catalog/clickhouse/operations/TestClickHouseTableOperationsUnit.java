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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
