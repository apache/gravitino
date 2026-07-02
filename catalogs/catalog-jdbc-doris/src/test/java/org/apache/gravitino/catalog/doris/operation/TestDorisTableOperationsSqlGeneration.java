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

import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.REPLICATION_ALLOCATION;
import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.REPLICATION_FACTOR;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
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
    }

    public void setDataSource(DataSource dataSource) {
      super.dataSource = dataSource;
    }

    public String createTableSql(
        String tableName, JdbcColumn[] columns, Distribution distribution) {
      return generateCreateTableSql(
          tableName,
          columns,
          "comment",
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          distribution,
          Indexes.EMPTY_INDEXES);
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
  public void testAppendNecessaryPropertiesAddsReplicationNumWhenBackendsAreNotEnough()
      throws Exception {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    ops.setDataSource(mockBackendDataSource(1));

    Map<String, String> properties = ops.appendNecessaryProperties(Collections.emptyMap());

    Assertions.assertEquals("1", properties.get(REPLICATION_FACTOR));
  }

  @Test
  public void testAppendNecessaryPropertiesKeepsReplicationAllocation() throws Exception {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    ops.setDataSource(mockBackendDataSource(1));

    Map<String, String> properties = new HashMap<>();
    properties.put(REPLICATION_ALLOCATION, "tag.location.default: 1");

    Map<String, String> result = ops.appendNecessaryProperties(properties);

    Assertions.assertEquals("tag.location.default: 1", result.get(REPLICATION_ALLOCATION));
    Assertions.assertFalse(result.containsKey(REPLICATION_FACTOR));
  }

  private static DataSource mockBackendDataSource(int aliveBackendCount) throws Exception {
    DataSource dataSource = Mockito.mock(DataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery("show backends")).thenReturn(resultSet);
    int[] remainingAliveBackends = new int[] {aliveBackendCount};
    Mockito.when(resultSet.next()).thenAnswer(invocation -> remainingAliveBackends[0]-- > 0);
    Mockito.when(resultSet.getString("Alive")).thenReturn("true");

    return dataSource;
  }
}
