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

package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTypeConverter;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/** Unit tests for Doris FE/JDBC physical schema validation. */
public class TestDorisPhysicalSchemaValidator35 {

  @Test
  void testOrdinaryScalarSchemaPasses() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("id", Types.IntegerType.get(), false);
    Table logicalTable = logicalTable(logicalColumn);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", false, 0));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", false, 0));

    assertDoesNotThrow(
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable,
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  @Test
  void testDorisDateV2AndTextAreOrdinaryTypes() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column dateColumn = logicalColumn("event_date", Types.DateType.get(), false);
    Column textColumn = logicalColumn("description", Types.StringType.get(), true);
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns()).thenReturn(new Column[] {dateColumn, textColumn});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("event_date", DataTypes.DateType, false),
              DataTypes.createStructField("description", DataTypes.StringType, true)
            });
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("event_date", "DATE", false, 0),
            new DorisPhysicalSchemaValidator35.PhysicalColumn("description", "TEXT", true, 1));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("event_date", "DATEV2", false, 0),
            new DorisPhysicalSchemaValidator35.PhysicalColumn("description", "STRING", true, 1));

    assertDoesNotThrow(
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable,
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  @Test
  void testTypeSignatureDriftFailsClosed() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("amount", Types.DecimalType.of(10, 2), false);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField(
                  "amount", DataTypes.createDecimalType(10, 2), false)
            });
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn(
                "amount", "DECIMAL(10,2)", false, 0));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn(
                "amount", "DECIMAL(20,2)", false, 0));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable(logicalColumn),
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  @Test
  void testFeAndJdbcTypeDriftFailsClosed() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("id", Types.IntegerType.get(), false);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", false, 0));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "BIGINT", false, 0));

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisPhysicalSchemaValidator35.validateColumns(
                    identifier,
                    logicalTable(logicalColumn),
                    physicalSchema,
                    jdbcColumns,
                    feColumns,
                    new SparkJdbcTypeConverter()));
    assertTrue(failure.getMessage().contains("FE and JDBC type families differ"));
  }

  @Test
  void testUnsupportedDorisTypeFailsClosed() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("payload", Types.StringType.get(), true);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("payload", DataTypes.StringType, true)});
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> columns =
        Arrays.asList(
            new DorisPhysicalSchemaValidator35.PhysicalColumn("payload", "JSON", true, 0));

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DorisPhysicalSchemaValidator35.validateColumns(
                    identifier,
                    logicalTable(logicalColumn),
                    physicalSchema,
                    columns,
                    columns,
                    new SparkJdbcTypeConverter()));
    assertTrue(failure.getMessage().contains("unsupported Doris type"));
  }

  @Test
  void testUnknownNullabilityFailsClosed() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "table");
    Column logicalColumn = logicalColumn("id", Types.IntegerType.get(), false);
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> jdbcColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", null, 0));
    List<DorisPhysicalSchemaValidator35.PhysicalColumn> feColumns =
        Arrays.asList(new DorisPhysicalSchemaValidator35.PhysicalColumn("id", "INT", false, 0));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisPhysicalSchemaValidator35.validateColumns(
                identifier,
                logicalTable(logicalColumn),
                physicalSchema,
                jdbcColumns,
                feColumns,
                new SparkJdbcTypeConverter()));
  }

  private static Column logicalColumn(
      String name, org.apache.gravitino.rel.types.Type type, boolean nullable) {
    Column column = mock(Column.class);
    when(column.name()).thenReturn(name);
    when(column.dataType()).thenReturn(type);
    when(column.nullable()).thenReturn(nullable);
    return column;
  }

  private static Table logicalTable(Column column) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(new Column[] {column});
    return table;
  }
}
