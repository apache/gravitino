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

import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.apache.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestDorisColumnMetadata {
  @Test
  void testNativeLargeIntTypeBeforeDefaultConversion() throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    CachedRowSet jdbcColumns = columns();
    addColumn(
        jdbcColumns, "target_table", "large", "INT", "170141183460469231731687303715884105727");
    addColumn(jdbcColumns, "target_table", "regular_int", "INT", "7");
    addColumn(jdbcColumns, "target_table", "regular_long", "BIGINT", "8");
    addColumn(jdbcColumns, "target_table", "unsigned_long", "BIGINT UNSIGNED", null);
    addColumn(jdbcColumns, "targetXtable", "large", "INT", "9");
    jdbcColumns.beforeFirst();
    when(metadata.getColumns(null, null, "target_table", null)).thenReturn(jdbcColumns);

    PreparedStatement statement = mock(PreparedStatement.class);
    ResultSet nativeColumns = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(nativeColumns);
    when(nativeColumns.next()).thenReturn(true, true, true, true, false);
    when(nativeColumns.getString("COLUMN_NAME"))
        .thenReturn("large", "regular_int", "regular_long", "unsigned_long");
    when(nativeColumns.getString("COLUMN_TYPE"))
        .thenReturn("LARGEINT", "int", "bigint", "bigint unsigned");

    TestableOperations operations = new TestableOperations();
    try (ResultSet result = operations.readMetadata(connection, "db", "target_table")) {
      Map<String, JdbcColumn> loaded = new HashMap<>();
      while (result.next()) {
        if ("targetXtable".equals(result.getString("TABLE_NAME"))) {
          assertEquals("INT", result.getString("TYPE_NAME"));
        } else {
          JdbcColumn column = operations.readColumn(result);
          loaded.put(column.name(), column);
        }
      }
      JdbcColumn large = loaded.get("large");
      assertEquals(Types.ExternalType.of("largeint"), large.dataType());
      assertEquals(
          UnparsedExpression.of("170141183460469231731687303715884105727"), large.defaultValue());
      assertEquals("column comment", large.comment());
      assertTrue(large.nullable());
      assertTrue(large.autoIncrement());
      assertEquals(Types.IntegerType.get(), loaded.get("regular_int").dataType());
      assertEquals(Types.LongType.get(), loaded.get("regular_long").dataType());
      assertEquals(
          Types.ExternalType.of("bigint unsigned"), loaded.get("unsigned_long").dataType());
    }
    verify(statement).setString(1, "db");
    verify(statement).setString(2, "target_table");
    verify(connection, times(1)).prepareStatement(anyString());
    verify(nativeColumns).close();
    verify(statement).close();
  }

  @Test
  void testNativeMetadataFailurePropagates() throws Exception {
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(metadata);
    CachedRowSet jdbcColumns = columns();
    when(metadata.getColumns(null, null, "t", null)).thenReturn(jdbcColumns);
    SQLException failure = new SQLException("metadata unavailable");
    when(connection.prepareStatement(anyString())).thenThrow(failure);
    assertSame(
        failure,
        assertThrows(
            SQLException.class,
            () -> new TestableOperations().readMetadata(connection, "db", "t")));
  }

  private static CachedRowSet columns() throws SQLException {
    String[] names = {
      "TABLE_NAME",
      "COLUMN_NAME",
      "TYPE_NAME",
      "COLUMN_SIZE",
      "DECIMAL_DIGITS",
      "REMARKS",
      "NULLABLE",
      "COLUMN_DEF",
      "IS_GENERATEDCOLUMN",
      "IS_AUTOINCREMENT"
    };
    RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
    metadata.setColumnCount(names.length);
    for (int i = 0; i < names.length; i++) {
      metadata.setColumnName(i + 1, names[i]);
      metadata.setColumnType(i + 1, VARCHAR);
    }
    CachedRowSet result = RowSetProvider.newFactory().createCachedRowSet();
    result.setMetaData(metadata);
    return result;
  }

  private static void addColumn(
      CachedRowSet rows, String table, String name, String type, String defaultValue)
      throws SQLException {
    rows.moveToInsertRow();
    String[] values = {
      table, name, type, "39", "0", "column comment", "1", defaultValue, "NO", "YES"
    };
    for (int i = 0; i < values.length; i++) {
      rows.updateString(i + 1, values[i]);
    }
    rows.insertRow();
    rows.moveToCurrentRow();
  }

  private static class TestableOperations extends DorisTableOperations {
    private TestableOperations() {
      typeConverter = new DorisTypeConverter();
      columnDefaultValueConverter = new DorisColumnDefaultValueConverter();
    }

    private ResultSet readMetadata(Connection connection, String database, String table)
        throws SQLException {
      return getColumns(connection, database, table);
    }

    private JdbcColumn readColumn(ResultSet result) throws SQLException {
      return getColumnBuilder(result, "db", "target_table")
          .withAutoIncrement(getAutoIncrementInfo(result))
          .build();
    }
  }
}
