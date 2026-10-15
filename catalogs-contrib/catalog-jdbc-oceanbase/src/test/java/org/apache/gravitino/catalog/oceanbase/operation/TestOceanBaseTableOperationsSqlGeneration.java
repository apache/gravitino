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
package org.apache.gravitino.catalog.oceanbase.operation;

import java.util.Collections;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseTypeConverter;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOceanBaseTableOperationsSqlGeneration {

  private static class TestableOceanBaseTableOperations extends OceanBaseTableOperations {
    public TestableOceanBaseTableOperations() {
      super.exceptionMapper = new JdbcExceptionConverter();
      super.typeConverter = new OceanBaseTypeConverter();
      super.columnDefaultValueConverter = new JdbcColumnDefaultValueConverter();
    }

    public String createTableSql(String tableName, JdbcColumn[] columns) {
      return generateCreateTableSql(
          tableName,
          columns,
          "comment",
          Collections.emptyMap(),
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);
    }
  }

  @Test
  public void testCreateTableWithEmptyStringDefaultValue() {
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("", Types.VarCharType.of(255)))
            .build();

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1});
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT '' but was: " + sql);
  }

  @Test
  public void testCreateTableWithNonEmptyStringDefaultValue() {
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
    String tableName = "test_table";
    JdbcColumn col1 =
        JdbcColumn.builder()
            .withName("col1")
            .withType(Types.VarCharType.of(255))
            .withNullable(false)
            .withDefaultValue(Literals.of("abc", Types.VarCharType.of(255)))
            .build();

    String sql = ops.createTableSql(tableName, new JdbcColumn[] {col1});
    JdbcColumnDefaultValueConverter converter = new JdbcColumnDefaultValueConverter();
    Assertions.assertTrue(
        sql.contains("DEFAULT " + converter.fromGravitino(col1.defaultValue())),
        "Should contain DEFAULT value but was: " + sql);
  }

  @Test
  public void testRejectNanosecondTimestampsBeforeSqlGeneration() {
    String expectedMessage =
        "OceanBase MySQL mode cannot preserve timestamp precision 9; "
            + "the maximum supported precision is 6";
    assertCreateTableRejected(Types.TimestampType.withoutTimeZone(9), expectedMessage);
    assertCreateTableRejected(Types.TimestampType.withTimeZone(9), expectedMessage);
  }

  @Test
  public void testRejectVariantBeforeSqlGeneration() {
    assertCreateTableRejected(
        Types.VariantType.get(),
        "OceanBase JSON cannot losslessly preserve the Gravitino variant type");
  }

  @Test
  public void testRejectUnknownBeforeSqlGeneration() {
    assertCreateTableRejected(
        Types.NullType.get(),
        "OceanBase has no column type that preserves the Gravitino unknown type");
  }

  @Test
  public void testRejectGeometryBeforeSqlGeneration() {
    assertCreateTableRejected(
        Types.GeometryType.crs84(),
        "OceanBase JDBC metadata cannot preserve Gravitino geometry CRS/SRID metadata");
  }

  @Test
  public void testRejectGeographyBeforeSqlGeneration() {
    assertCreateTableRejected(
        Types.GeographyType.crs84(),
        "OceanBase has no geography column type that preserves CRS and edge algorithm metadata");
  }

  private void assertCreateTableRejected(Type type, String expectedMessage) {
    TestableOceanBaseTableOperations ops = new TestableOceanBaseTableOperations();
    JdbcColumn column =
        JdbcColumn.builder().withName("col1").withType(type).withNullable(true).build();

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> ops.createTableSql("test_table", new JdbcColumn[] {column}));
    Assertions.assertEquals(expectedMessage, exception.getMessage());
  }
}
