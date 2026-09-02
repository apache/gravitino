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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests logical/physical schema validation and SQL normalization planning. */
public class TestDorisSchemaCompatibility35 {

  @Test
  void testExternalTypeUsesSqlStringProjection() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of("id", Types.IntegerType.get()),
              Column.of("payload", Types.ExternalType.of("json"))
            });
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("payload", DataTypes.StringType, true)
            });

    DorisReadSchema35 result =
        DorisSchemaCompatibility35.plan(
            Identifier.of(new String[] {"db"}, "t"),
            logicalTable,
            new DorisPhysicalSchema35(physicalSchema, Arrays.asList("INT", "JSON")),
            new DorisSparkTypeConverter35());

    assertTrue(result.requiresSqlExecution());
    assertEquals(DataTypes.StringType, result.schema().fields()[1].dataType());
    assertTrue(result.projections().get(1).contains("CAST"));
    assertEquals("JSON", result.normalizedTypeName("payload"));
  }

  @Test
  void testNormalizedTypeDriftFailsClosed() {
    Table external = mock(Table.class);
    when(external.columns())
        .thenReturn(new Column[] {Column.of("payload", Types.ExternalType.of("json"))});
    StructType stringSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("payload", DataTypes.StringType, true)});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                external,
                new DorisPhysicalSchema35(stringSchema, Arrays.asList("HLL")),
                new DorisSparkTypeConverter35()));

    Table wrongLogicalType = mock(Table.class);
    when(wrongLogicalType.columns())
        .thenReturn(new Column[] {Column.of("payload", Types.BooleanType.get())});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                wrongLogicalType,
                new DorisPhysicalSchema35(stringSchema, Arrays.asList("JSON")),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testDatetimePrecisionDriftFailsClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("event_time", Types.TimestampType.withoutTimeZone(6))});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("event_time", DataTypes.TimestampType, true)
            });

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(physicalSchema, Arrays.asList("DATETIMEV2(3)")),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testDirectTypeMismatchFailsClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("id", Types.IntegerType.get())});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.StringType, true)});

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(physicalSchema, Arrays.asList("VARCHAR")),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testCharacterLengthMismatchFailsClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("name", Types.VarCharType.of(64))});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("name", DataTypes.StringType, true)});

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(physicalSchema, Arrays.asList("VARCHAR(32)")),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testBinaryTypeUsesBase64Projection() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("payload", Types.BinaryType.get())});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("payload", DataTypes.BinaryType, true)});

    DorisReadSchema35 result =
        DorisSchemaCompatibility35.plan(
            Identifier.of(new String[] {"db"}, "t"),
            logicalTable,
            new DorisPhysicalSchema35(physicalSchema, Arrays.asList("BINARY")),
            new DorisSparkTypeConverter35());

    assertEquals(DataTypes.StringType, result.schema().fields()[0].dataType());
    assertEquals("TO_BASE64(`payload`) AS `payload`", result.projections().get(0));
  }

  @ParameterizedTest
  @ValueSource(strings = {"LARGEINT", "JSON", "VARIANT", "IPV4", "IPV6", "DECIMAL256"})
  void testDorisSpecialTypesUseSqlStringProjection(String dorisType) {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {Column.of("payload", Types.ExternalType.of(dorisType.toLowerCase()))});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("payload", DataTypes.StringType, true)});

    DorisReadSchema35 result =
        DorisSchemaCompatibility35.plan(
            Identifier.of(new String[] {"db"}, "t"),
            logicalTable,
            new DorisPhysicalSchema35(physicalSchema, Arrays.asList(dorisType)),
            new DorisSparkTypeConverter35());

    assertTrue(result.requiresSqlExecution());
    assertEquals(DataTypes.StringType, result.schema().fields()[0].dataType());
    assertTrue(result.projections().get(0).contains("CAST"));
  }

  @Test
  void testLargeIntUsesCertifiedJdbcLogicalFallback() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("large_value", Types.IntegerType.get())});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("large_value", DataTypes.StringType, true)
            });

    DorisReadSchema35 result =
        DorisSchemaCompatibility35.plan(
            Identifier.of(new String[] {"db"}, "t"),
            logicalTable,
            new DorisPhysicalSchema35(physicalSchema, Arrays.asList("LARGEINT")),
            new DorisSparkTypeConverter35());

    assertEquals(DataTypes.StringType, result.schema().fields()[0].dataType());
    assertEquals("LARGEINT", result.normalizedTypeName("large_value"));
  }

  @Test
  void testColumnCountAndNullabilityMismatchFailClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {
              Column.of("id", Types.IntegerType.get()),
              Column.of("name", Types.StringType.get(), null, false, false, null)
            });
    StructType oneColumnSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(oneColumnSchema, Arrays.asList("INT")),
                new DorisSparkTypeConverter35()));

    when(logicalTable.columns())
        .thenReturn(
            new Column[] {Column.of("id", Types.IntegerType.get(), null, true, false, null)});
    StructType nonNullableSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, false)});
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(nonNullableSchema, Arrays.asList("INT")),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testUnknownPhysicalTypeFailsClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("payload", Types.StringType.get())});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("payload", DataTypes.StringType, true)});

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(
                    physicalSchema,
                    Arrays.asList("MYSTERY_TYPE"),
                    Arrays.asList(false),
                    Arrays.asList(true)),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testMalformedDecimalFallbackFailsClosed() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(new Column[] {Column.of("amount", Types.ExternalType.of("decimal"))});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("amount", DataTypes.StringType, true)});

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisSchemaCompatibility35.plan(
                Identifier.of(new String[] {"db"}, "t"),
                logicalTable,
                new DorisPhysicalSchema35(
                    physicalSchema,
                    Arrays.asList("DECIMAL(not-a-number,2)"),
                    Arrays.asList(false),
                    Arrays.asList(true)),
                new DorisSparkTypeConverter35()));
  }

  @Test
  void testUnknownPhysicalNullabilityUsesLogicalContract() {
    Table logicalTable = mock(Table.class);
    when(logicalTable.columns())
        .thenReturn(
            new Column[] {Column.of("id", Types.IntegerType.get(), null, false, false, null)});
    StructType physicalSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("id", DataTypes.IntegerType, true)});

    DorisReadSchema35 result =
        DorisSchemaCompatibility35.plan(
            Identifier.of(new String[] {"db"}, "t"),
            logicalTable,
            new DorisPhysicalSchema35(
                physicalSchema, Arrays.asList("INT"), Arrays.asList(true), Arrays.asList(false)),
            new DorisSparkTypeConverter35());

    assertFalse(result.schema().fields()[0].nullable());
  }
}
