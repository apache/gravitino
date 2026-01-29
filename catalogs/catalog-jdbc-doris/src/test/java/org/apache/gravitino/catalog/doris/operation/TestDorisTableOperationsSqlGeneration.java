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

import java.util.Collections;
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
            .withType(Types.VarCharType.of(255))
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
    Assertions.assertTrue(sql.contains("DEFAULT ''"), "Should contain DEFAULT '' but was: " + sql);
  }

  @Test
  public void testCreateTableWithNonEmptyStringDefaultValue() {
    TestableDorisTableOperations ops = new TestableDorisTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
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
    Assertions.assertTrue(
        sql.contains("DEFAULT 'abc'"), "Should contain DEFAULT 'abc' but was: " + sql);
  }
}
