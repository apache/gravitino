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
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.TableConstants;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.expressions.FunctionExpression;
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

    SystemTableMetadata callGetSystemTableMetadata(
        Connection connection, String databaseName, String tableName) throws Exception {
      return getSystemTableMetadata(connection, databaseName, tableName);
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

    String primaryKeySql = sqlCaptor.getAllValues().get(0);
    Assertions.assertTrue(
        primaryKeySql.contains("db''1"), "database single quote should be doubled");
    Assertions.assertTrue(primaryKeySql.contains("t''1"), "table single quote should be doubled");
  }

  @Test
  void testGetSystemTableMetadataQueriesExactTable() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.when(connection.prepareStatement(sqlCaptor.capture())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true);
    Mockito.when(resultSet.getString("sorting_key")).thenReturn("id");
    Mockito.when(resultSet.getString("engine_full")).thenReturn("MergeTree ORDER BY id");

    ClickHouseTableOperations.SystemTableMetadata metadata =
        ops.callGetSystemTableMetadata(connection, "db_name", "table_name");

    Assertions.assertEquals(
        "SELECT sorting_key, engine_full FROM system.tables WHERE database = ? AND name = ?",
        sqlCaptor.getValue());
    Mockito.verify(statement).setString(1, "db_name");
    Mockito.verify(statement).setString(2, "table_name");
    Assertions.assertEquals(1, metadata.sortOrders().length);
    Assertions.assertEquals(NamedReference.field("id"), metadata.sortOrders()[0].expression());
    Assertions.assertTrue(metadata.settings().isEmpty());
    Mockito.verify(resultSet).close();
    Mockito.verify(statement).close();
  }

  @Test
  void testGetSystemTableMetadataParsesCompoundAndFunctionExpressions() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true);
    Mockito.when(resultSet.getString("sorting_key")).thenReturn("id, toDate(event_time)");
    Mockito.when(resultSet.getString("engine_full")).thenReturn("MergeTree ORDER BY id");

    SortOrder[] sortOrders =
        ops.callGetSystemTableMetadata(connection, "db_name", "table_name").sortOrders();

    Assertions.assertEquals(2, sortOrders.length);
    Assertions.assertEquals(NamedReference.field("id"), sortOrders[0].expression());
    Assertions.assertEquals(
        FunctionExpression.of("toDate", NamedReference.field("event_time")),
        sortOrders[1].expression());
  }

  @Test
  void testGetSystemTableMetadataReturnsNoneForBlankValues() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true);
    Mockito.when(resultSet.getString("sorting_key")).thenReturn("   ");
    Mockito.when(resultSet.getString("engine_full")).thenReturn("   ");

    ClickHouseTableOperations.SystemTableMetadata metadata =
        ops.callGetSystemTableMetadata(connection, "db_name", "table_name");

    Assertions.assertArrayEquals(new SortOrder[0], metadata.sortOrders());
    Assertions.assertTrue(metadata.settings().isEmpty());
  }

  @Test
  void testGetSystemTableMetadataParsesSettingsFromEngineFull() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true);
    Mockito.when(resultSet.getString("sorting_key")).thenReturn("id");
    Mockito.when(resultSet.getString("engine_full"))
        .thenReturn(
            "MergeTree ORDER BY id SETTINGS index_granularity = 4096, "
                + "min_bytes_for_wide_part = 0");

    Map<String, String> settings =
        ops.callGetSystemTableMetadata(connection, "db_name", "table_name").settings();

    Assertions.assertEquals(2, settings.size());
    Assertions.assertEquals(
        "4096", settings.get(TableConstants.SETTINGS_PREFIX + "index_granularity"));
    Assertions.assertEquals(
        "0", settings.get(TableConstants.SETTINGS_PREFIX + "min_bytes_for_wide_part"));
  }

  @Test
  void testGetSystemTableMetadataThrowsWhenTableIsNotVisible() throws Exception {
    ExposedClickHouseTableOperations ops = newOps();
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement statement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
    Mockito.when(statement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(false);

    NoSuchTableException exception =
        Assertions.assertThrows(
            NoSuchTableException.class,
            () -> ops.callGetSystemTableMetadata(connection, "db_name", "table_name"));

    Assertions.assertTrue(exception.getMessage().contains("table_name"));
    Assertions.assertTrue(exception.getMessage().contains("db_name"));
  }
}
