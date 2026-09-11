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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

/** Tests the narrow governed Doris write-schema contract. */
public class TestDorisWriteSchemaCompatibility35 {

  @Test
  void testExactTypesAndDatetimeStrings() {
    Table direct =
        table(
            column("id", Types.IntegerType.get(), false),
            column("name", Types.StringType.get(), true));
    StructType directSchema =
        schema(
            field("id", DataTypes.IntegerType, false), field("name", DataTypes.StringType, true));
    assertDoesNotThrow(
        () ->
            DorisWriteSchemaCompatibility35.validate(
                direct, readSchema(directSchema, ImmutableMap.of()), directSchema));

    Table datetime = table(column("event_time", Types.TimestampType.withoutTimeZone(6), true));
    StructType stringSchema = schema(field("event_time", DataTypes.StringType, true));
    DorisWriteSchemaCompatibility35.Validator validator =
        DorisWriteSchemaCompatibility35.validate(
            datetime,
            readSchema(stringSchema, ImmutableMap.of("event_time", "DATETIMEV2(6)")),
            stringSchema);
    assertDoesNotThrow(() -> validator.validate(row("2024-02-29 23:59:59.123456")));
    assertDoesNotThrow(() -> validator.validate(new GenericInternalRow(new Object[] {null})));
  }

  @Test
  void testDatetimeValidationDoesNotLeakValues() {
    Table datetime = table(column("event_time", Types.TimestampType.withoutTimeZone(3), true));
    StructType stringSchema = schema(field("event_time", DataTypes.StringType, true));
    DorisWriteSchemaCompatibility35.Validator validator =
        DorisWriteSchemaCompatibility35.validate(
            datetime,
            readSchema(stringSchema, ImmutableMap.of("event_time", "DATETIMEV2(3)")),
            stringSchema);

    for (String invalid :
        ImmutableList.of(
            "2024-02-30 00:00:00.001",
            "2024-02-29T00:00:00.001",
            "2024-02-29 00:00:00.01",
            "secret-value")) {
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> validator.validate(row(invalid)));
      assertFalse(error.getMessage().contains(invalid));
    }
  }

  @Test
  void testRejectsSchemaDriftAndLossyNormalizedTypes() {
    Table target = table(column("id", Types.IntegerType.get(), false));
    DorisReadSchema35 read =
        readSchema(schema(field("id", DataTypes.IntegerType, false)), ImmutableMap.of());

    assertThrows(
        IllegalArgumentException.class,
        () -> DorisWriteSchemaCompatibility35.validate(target, read, new StructType()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                target, read, schema(field("other", DataTypes.IntegerType, false))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                target, read, schema(field("ID", DataTypes.IntegerType, false))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                target, read, schema(field("id", DataTypes.LongType, false))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                target, read, schema(field("id", DataTypes.IntegerType, true))));

    Table binary = table(column("payload", Types.BinaryType.get(), true));
    StructType normalizedSchema = schema(field("payload", DataTypes.StringType, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                binary,
                readSchema(normalizedSchema, ImmutableMap.of("payload", "BINARY")),
                normalizedSchema));

    Table ordered =
        table(
            column("id", Types.IntegerType.get(), false),
            column("name", Types.StringType.get(), true));
    StructType orderedSchema =
        schema(
            field("id", DataTypes.IntegerType, false), field("name", DataTypes.StringType, true));
    StructType reversedSchema =
        schema(
            field("name", DataTypes.StringType, true), field("id", DataTypes.IntegerType, false));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                ordered, readSchema(orderedSchema, ImmutableMap.of()), reversedSchema));

    Table timezone = table(column("event_time", Types.TimestampType.withTimeZone(3), true));
    StructType datetimeSchema = schema(field("event_time", DataTypes.StringType, true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                timezone,
                readSchema(datetimeSchema, ImmutableMap.of("event_time", "DATETIMEV2(3)")),
                datetimeSchema));

    Table unsupportedPrecision =
        table(column("event_time", Types.TimestampType.withoutTimeZone(7), true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                unsupportedPrecision,
                readSchema(datetimeSchema, ImmutableMap.of("event_time", "DATETIMEV2(7)")),
                datetimeSchema));

    Table precisionMismatch =
        table(column("event_time", Types.TimestampType.withoutTimeZone(6), true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWriteSchemaCompatibility35.validate(
                precisionMismatch,
                readSchema(datetimeSchema, ImmutableMap.of("event_time", "DATETIMEV2(3)")),
                datetimeSchema));
  }

  private static DorisReadSchema35 readSchema(
      StructType schema, Map<String, String> normalizedTypeNames) {
    ImmutableList.Builder<String> projections = ImmutableList.builder();
    for (String fieldName : schema.fieldNames()) {
      projections.add(DorisReadSchema35.quoteIdentifier(fieldName));
    }
    return new DorisReadSchema35(
        schema, projections.build(), !normalizedTypeNames.isEmpty(), normalizedTypeNames);
  }

  private static Table table(Column... columns) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(columns);
    return table;
  }

  private static Column column(String name, Type type, boolean nullable) {
    return Column.of(name, type, null, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static StructField field(String name, DataType type, boolean nullable) {
    return DataTypes.createStructField(name, type, nullable);
  }

  private static StructType schema(StructField... fields) {
    return DataTypes.createStructType(fields);
  }

  private static GenericInternalRow row(String value) {
    return new GenericInternalRow(new Object[] {UTF8String.fromString(value)});
  }
}
