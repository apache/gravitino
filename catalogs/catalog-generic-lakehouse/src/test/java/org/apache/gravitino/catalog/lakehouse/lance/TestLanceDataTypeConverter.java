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
package org.apache.gravitino.catalog.lakehouse.lance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
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

  @Test
  void testToGravitinoNotImplemented() {
    assertThrows(
        UnsupportedOperationException.class, () -> CONVERTER.toGravitino(ArrowType.Utf8.INSTANCE));
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
        Arguments.of("items", LIST_OF_STRUCTS, false, LIST_OF_STRUCTS_VALIDATOR));
  }
}
