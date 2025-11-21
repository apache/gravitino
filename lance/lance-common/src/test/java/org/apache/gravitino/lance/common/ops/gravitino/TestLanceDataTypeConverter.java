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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestLanceDataTypeConverter {
  private static final LanceDataTypeConverter CONVERTER = LanceDataTypeConverter.CONVERTER;

  // Gravitino complex type definitions for testing
  private static final Types.StructType SIMPLE_STRUCT =
      Types.StructType.of(
          Types.StructType.Field.of("id", Types.LongType.get(), false, null),
          Types.StructType.Field.of("name", Types.StringType.get(), true, null));

  private static final Types.StructType NESTED_STRUCT =
      Types.StructType.of(
          Types.StructType.Field.of("id", Types.LongType.get(), false, null),
          Types.StructType.Field.of(
              "address",
              Types.StructType.of(
                  Types.StructType.Field.of("street", Types.StringType.get(), false, null),
                  Types.StructType.Field.of("city", Types.StringType.get(), false, null)),
              true,
              null));
  private static final String NESTED_STRUCT_JSON =
      "{\"name\":\"person_nested_json\",\"nullable\":false,\"type\":{\"name\":\"struct\"},\"children\":["
          + "{\"name\":\"id\",\"nullable\":false,\"type\":{\"name\":\"int\",\"bitWidth\":64,\"isSigned\":true},\"children\":[]},"
          + "{\"name\":\"address\",\"nullable\":true,\"type\":{\"name\":\"struct\"},\"children\":["
          + "{\"name\":\"street\",\"nullable\":false,\"type\":{\"name\":\"utf8\"},\"children\":[]},"
          + "{\"name\":\"city\",\"nullable\":false,\"type\":{\"name\":\"utf8\"},\"children\":[]}"
          + "]}"
          + "]}";

  private static final Types.ListType LIST_OF_STRUCTS =
      Types.ListType.of(
          Types.StructType.of(
              Types.StructType.Field.of("sku", Types.StringType.get(), false, null),
              Types.StructType.Field.of("quantity", Types.IntegerType.get(), false, null)),
          true);

  // Field validators for Arrow conversion tests
  private static Consumer<Field> INT_VALIDATOR =
      field -> assertInstanceOf(ArrowType.Int.class, field.getFieldType().getType());
  private static Consumer<Field> STRING_VALIDATOR =
      field -> assertInstanceOf(ArrowType.Utf8.class, field.getFieldType().getType());
  private static Consumer<Field> LARGE_UTF8_VALIDATOR =
      field -> assertInstanceOf(ArrowType.LargeUtf8.class, field.getFieldType().getType());
  private static Consumer<Field> BOOLEAN_VALIDATOR =
      field -> assertInstanceOf(ArrowType.Bool.class, field.getFieldType().getType());
  private static Consumer<Field> DECIMAL_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.Decimal.class, field.getFieldType().getType());
        ArrowType.Decimal decimal = (ArrowType.Decimal) field.getFieldType().getType();

        assertEquals(10, decimal.getPrecision());
        assertEquals(2, decimal.getScale());
      };
  private static Consumer<Field> LIST_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.List.class, field.getFieldType().getType());
        assertEquals(1, field.getChildren().size());

        Field elementField = field.getChildren().get(0);
        assertEquals("element", elementField.getName());
        assertTrue(elementField.isNullable());
        assertInstanceOf(ArrowType.Int.class, elementField.getFieldType().getType());
      };
  private static Consumer<Field> MAP_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.Map.class, field.getFieldType().getType());
        assertEquals(1, field.getChildren().size());

        Field structField = field.getChildren().get(0);
        assertEquals(MapVector.DATA_VECTOR_NAME, structField.getName());
        assertEquals(2, structField.getChildren().size());

        Field keyField = structField.getChildren().get(0);
        assertEquals(MapVector.KEY_NAME, keyField.getName());
        assertFalse(keyField.isNullable());
        assertInstanceOf(ArrowType.Utf8.class, keyField.getFieldType().getType());

        Field valueField = structField.getChildren().get(1);
        assertEquals(MapVector.VALUE_NAME, valueField.getName());
        assertTrue(valueField.isNullable());
        assertInstanceOf(ArrowType.Int.class, valueField.getFieldType().getType());
      };
  private static Consumer<Field> STRUCT_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.Struct.class, field.getFieldType().getType());
        assertEquals(2, field.getChildren().size());

        Field idField = field.getChildren().get(0);
        assertEquals("id", idField.getName());
        assertFalse(idField.isNullable());
        assertInstanceOf(ArrowType.Int.class, idField.getFieldType().getType());

        Field nameField = field.getChildren().get(1);
        assertEquals("name", nameField.getName());
        assertTrue(nameField.isNullable());
        assertInstanceOf(ArrowType.Utf8.class, nameField.getFieldType().getType());
      };
  private static Consumer<Field> NESTED_STRUCT_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.Struct.class, field.getFieldType().getType());
        assertEquals(2, field.getChildren().size());

        Field addressField = field.getChildren().get(1);
        assertEquals("address", addressField.getName());
        assertTrue(addressField.isNullable());

        assertInstanceOf(ArrowType.Struct.class, addressField.getFieldType().getType());
        assertEquals(2, addressField.getChildren().size());
      };
  private static Consumer<Field> LIST_OF_STRUCTS_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.List.class, field.getFieldType().getType());
        assertEquals(1, field.getChildren().size());

        Field elementField = field.getChildren().get(0);
        assertEquals("element", elementField.getName());
        assertTrue(elementField.isNullable());
        assertInstanceOf(ArrowType.Struct.class, elementField.getFieldType().getType());
        assertEquals(2, elementField.getChildren().size());
      };
  private static Consumer<Field> UNION_VALIDATOR =
      field -> {
        assertInstanceOf(ArrowType.Union.class, field.getFieldType().getType());
        ArrowType.Union unionType = (ArrowType.Union) field.getFieldType().getType();
        assertEquals(UnionMode.Sparse, unionType.getMode());
        assertEquals(2, field.getChildren().size());
        assertInstanceOf(ArrowType.Int.class, field.getChildren().get(0).getFieldType().getType());
        assertInstanceOf(ArrowType.Utf8.class, field.getChildren().get(1).getFieldType().getType());
      };

  @ParameterizedTest
  @DisplayName("Test conversion of Integer types (Byte, Short, Integer, Long)")
  @CsvSource({"BYTE, 8, true", "SHORT, 16, true", "INTEGER, 32, true", "LONG, 64, true"})
  public void testFromGravitinoIntegerTypes(
      String typeName, int expectedBitWidth, boolean expectedSigned) {
    Type type =
        switch (typeName) {
          case "BYTE" -> Types.ByteType.get();
          case "SHORT" -> Types.ShortType.get();
          case "INTEGER" -> Types.IntegerType.get();
          case "LONG" -> Types.LongType.get();
          default -> throw new IllegalArgumentException("Unknown type: " + typeName);
        };

    ArrowType arrowType = CONVERTER.fromGravitino(type);
    assertInstanceOf(ArrowType.Int.class, arrowType);

    ArrowType.Int intType = (ArrowType.Int) arrowType;
    assertEquals(expectedBitWidth, intType.getBitWidth());
    assertEquals(expectedSigned, intType.getIsSigned());
  }

  @Test
  public void testFromGravitinoTimestampWithTz() {
    Types.TimestampType timestampType = Types.TimestampType.withTimeZone();
    ArrowType arrowType = CONVERTER.fromGravitino(timestampType);
    assertInstanceOf(ArrowType.Timestamp.class, arrowType);

    ArrowType.Timestamp tsArrow = (ArrowType.Timestamp) arrowType;
    assertEquals(TimeUnit.MICROSECOND, tsArrow.getUnit());
    assertEquals("UTC", tsArrow.getTimezone());
  }

  @Test
  public void testExternalTypeConversion() {
    String expectedColumnName = "col_name";
    boolean expectedNullable = true;
    Types.ExternalType externalType =
        Types.ExternalType.of(
            "{\"name\":\"col_name\",\"nullable\":true,"
                + "\"type\":{\"name\":\"largeutf8\"},\"children\":[]}");
    Field arrowField = CONVERTER.toArrowField(expectedColumnName, externalType, expectedNullable);
    assertEquals(expectedColumnName, arrowField.getName());
    assertEquals(expectedNullable, arrowField.isNullable());
    assertInstanceOf(ArrowType.LargeUtf8.class, arrowField.getFieldType().getType());

    externalType =
        Types.ExternalType.of(
            "{\"name\":\"col_name\",\"nullable\":true,"
                + "\"type\":{\"name\":\"largebinary\"},\"children\":[]}");
    arrowField = CONVERTER.toArrowField(expectedColumnName, externalType, expectedNullable);
    assertEquals(expectedColumnName, arrowField.getName());
    assertEquals(expectedNullable, arrowField.isNullable());
    assertInstanceOf(ArrowType.LargeBinary.class, arrowField.getFieldType().getType());

    externalType =
        Types.ExternalType.of(
            "{\"name\":\"col_name\",\"nullable\":true,"
                + "\"type\":{\"name\":\"largelist\"},"
                + "\"children\":["
                + "{\"name\":\"element\",\"nullable\":true,"
                + "\"type\":{\"name\":\"int\", \"bitWidth\":32, \"isSigned\": true},"
                + "\"children\":[]}]}");
    arrowField = CONVERTER.toArrowField(expectedColumnName, externalType, expectedNullable);
    assertEquals(expectedColumnName, arrowField.getName());
    assertEquals(expectedNullable, arrowField.isNullable());
    assertInstanceOf(ArrowType.LargeList.class, arrowField.getFieldType().getType());

    externalType =
        Types.ExternalType.of(
            "{\"name\":\"col_name\",\"nullable\":true,"
                + "\"type\":{\"name\":\"fixedsizelist\", \"listSize\":10},"
                + "\"children\":["
                + "{\"name\":\"element\",\"nullable\":true,"
                + "\"type\":{\"name\":\"int\", \"bitWidth\":32, \"isSigned\": true},"
                + "\"children\":[]}]}");
    arrowField = CONVERTER.toArrowField(expectedColumnName, externalType, expectedNullable);
    assertEquals(expectedColumnName, arrowField.getName());
    assertEquals(expectedNullable, arrowField.isNullable());
    assertInstanceOf(ArrowType.FixedSizeList.class, arrowField.getFieldType().getType());
    assertEquals(10, ((ArrowType.FixedSizeList) arrowField.getFieldType().getType()).getListSize());
  }

  @ParameterizedTest(name = "[{index}] name={0}, type={1}, nullable={2}")
  @MethodSource("toArrowFieldArguments")
  @DisplayName("Test toArrowField for various types")
  public void testToArrowField(
      String name, Type gravitinoType, boolean nullable, Consumer<Field> validator) {
    Field field = CONVERTER.toArrowField(name, gravitinoType, nullable);

    assertEquals(name, field.getName());
    assertEquals(nullable, field.isNullable());
    validator.accept(field);
  }

  @Test
  void testUnsupportedTypeThrowsException() {
    Types.UnparsedType unparsedType = Types.UnparsedType.of("UNKNOWN_TYPE");
    assertThrows(UnsupportedOperationException.class, () -> CONVERTER.fromGravitino(unparsedType));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("toGravitinoArguments")
  void testToGravitino(String testName, Field arrowField, Type expectedGravitinoType) {
    Type convertedType = CONVERTER.toGravitino(arrowField);
    assertEquals(expectedGravitinoType, convertedType);
  }

  private static Stream<Arguments> toArrowFieldArguments() {
    return Stream.of(
        // Simple types
        Arguments.of("age", Types.IntegerType.get(), true, INT_VALIDATOR),
        Arguments.of("id", Types.LongType.get(), false, INT_VALIDATOR),
        Arguments.of("name", Types.StringType.get(), true, STRING_VALIDATOR),
        Arguments.of(
            "description",
            Types.ExternalType.of(
                "{\n"
                    + "  \"name\": \"description\",\n"
                    + "  \"nullable\": true,\n"
                    + "  \"type\": {\n"
                    + "    \"name\": \"largeutf8\"\n"
                    + "  }\n"
                    + "}"),
            true,
            LARGE_UTF8_VALIDATOR),
        Arguments.of("active", Types.BooleanType.get(), false, BOOLEAN_VALIDATOR),
        // Decimal
        Arguments.of("price", Types.DecimalType.of(10, 2), false, DECIMAL_VALIDATOR),
        // List
        Arguments.of(
            "numbers", Types.ListType.of(Types.IntegerType.get(), true), false, LIST_VALIDATOR),
        // Map
        Arguments.of(
            "properties",
            Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true),
            true,
            MAP_VALIDATOR),
        // Struct
        Arguments.of("person", SIMPLE_STRUCT, true, STRUCT_VALIDATOR),
        // Nested Struct
        Arguments.of("person_nested", NESTED_STRUCT, false, NESTED_STRUCT_VALIDATOR),
        Arguments.of(
            "person_nested_json",
            Types.ExternalType.of(NESTED_STRUCT_JSON),
            false,
            NESTED_STRUCT_VALIDATOR),
        // List of Structs
        Arguments.of("items", LIST_OF_STRUCTS, false, LIST_OF_STRUCTS_VALIDATOR),
        // Union
        Arguments.of(
            "union_field",
            Types.UnionType.of(Types.IntegerType.get(), Types.StringType.get()),
            true,
            UNION_VALIDATOR));
  }

  private static Stream<Arguments> toGravitinoArguments() {
    return Stream.of(
        // Simple Types
        Arguments.of(
            "Boolean",
            new Field("bool_col", new FieldType(true, ArrowType.Bool.INSTANCE, null), null),
            Types.BooleanType.get()),
        Arguments.of(
            "Byte",
            new Field("byte_col", new FieldType(true, new ArrowType.Int(8, true), null), null),
            Types.ByteType.get()),
        Arguments.of(
            "Short",
            new Field("short_col", new FieldType(true, new ArrowType.Int(16, true), null), null),
            Types.ShortType.get()),
        Arguments.of(
            "Integer",
            new Field("int_col", new FieldType(true, new ArrowType.Int(32, true), null), null),
            Types.IntegerType.get()),
        Arguments.of(
            "Long",
            new Field("long_col", new FieldType(true, new ArrowType.Int(64, true), null), null),
            Types.LongType.get()),
        Arguments.of(
            "Float",
            new Field(
                "float_col",
                new FieldType(
                    true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
                null),
            Types.FloatType.get()),
        Arguments.of(
            "Double",
            new Field(
                "double_col",
                new FieldType(
                    true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
                null),
            Types.DoubleType.get()),
        Arguments.of(
            "String",
            new Field("string_col", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null),
            Types.StringType.get()),
        Arguments.of(
            "Binary",
            new Field("binary_col", new FieldType(true, ArrowType.Binary.INSTANCE, null), null),
            Types.BinaryType.get()),
        Arguments.of(
            "Decimal",
            new Field(
                "decimal_col", new FieldType(true, new ArrowType.Decimal(10, 2, 128), null), null),
            Types.DecimalType.of(10, 2)),
        Arguments.of(
            "Date",
            new Field(
                "date_col", new FieldType(true, new ArrowType.Date(DateUnit.DAY), null), null),
            Types.DateType.get()),
        Arguments.of(
            "Timestamp without TZ",
            new Field(
                "ts_col",
                new FieldType(true, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), null),
                null),
            Types.TimestampType.withoutTimeZone(6)),
        Arguments.of(
            "Timestamp with TZ",
            new Field(
                "tstz_col",
                new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null),
                null),
            Types.TimestampType.withTimeZone(3)),
        Arguments.of(
            "Time",
            new Field(
                "time_col",
                new FieldType(true, new ArrowType.Time(TimeUnit.NANOSECOND, 64), null),
                null),
            Types.TimeType.get()),
        Arguments.of(
            "Fixed",
            new Field(
                "fixed_col", new FieldType(true, new ArrowType.FixedSizeBinary(16), null), null),
            Types.FixedType.of(16)),
        // Complex Types
        Arguments.of(
            "List",
            new Field(
                "list_col",
                new FieldType(false, ArrowType.List.INSTANCE, null),
                Collections.singletonList(
                    new Field(
                        "element", new FieldType(true, new ArrowType.Int(32, true), null), null))),
            Types.ListType.of(Types.IntegerType.get(), true)),
        Arguments.of(
            "Map",
            new Field(
                "map_col",
                new FieldType(true, new ArrowType.Map(false), null),
                Collections.singletonList(
                    new Field(
                        MapVector.DATA_VECTOR_NAME,
                        new FieldType(false, ArrowType.Struct.INSTANCE, null),
                        Arrays.asList(
                            new Field(
                                MapVector.KEY_NAME,
                                new FieldType(false, ArrowType.Utf8.INSTANCE, null),
                                null),
                            new Field(
                                MapVector.VALUE_NAME,
                                new FieldType(true, new ArrowType.Int(32, true), null),
                                null))))),
            Types.MapType.of(Types.StringType.get(), Types.IntegerType.get(), true)),
        Arguments.of(
            "Struct",
            new Field(
                "struct_col",
                new FieldType(true, ArrowType.Struct.INSTANCE, null),
                Arrays.asList(
                    new Field("id", new FieldType(false, new ArrowType.Int(64, true), null), null),
                    new Field("name", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null))),
            SIMPLE_STRUCT),
        Arguments.of(
            "Union",
            new Field(
                "union_col",
                new FieldType(true, new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2}), null),
                Arrays.asList(
                    new Field(
                        "integer", new FieldType(true, new ArrowType.Int(32, true), null), null),
                    new Field("string", new FieldType(true, ArrowType.Utf8.INSTANCE, null), null))),
            Types.UnionType.of(Types.IntegerType.get(), Types.StringType.get())),
        // External Type
        Arguments.of(
            "External (LargeUtf8)",
            new Field(
                "external_col", new FieldType(true, ArrowType.LargeUtf8.INSTANCE, null), null),
            Types.ExternalType.of(
                "{\"name\":\"external_col\",\"nullable\":true,\"type\":{\"name\":\"largeutf8\"},\"children\":[]}")));
  }
}
