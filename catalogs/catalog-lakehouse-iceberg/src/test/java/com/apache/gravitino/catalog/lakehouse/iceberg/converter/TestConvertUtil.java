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
package com.apache.gravitino.catalog.lakehouse.iceberg.converter;

import static com.apache.gravitino.catalog.lakehouse.iceberg.converter.IcebergDataTypeConverter.CONVERTER;

import com.apache.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.apache.gravitino.meta.AuditInfo;
import com.apache.gravitino.rel.Column;
import com.apache.gravitino.rel.expressions.sorts.SortOrder;
import com.apache.gravitino.rel.types.Types.ByteType;
import com.apache.gravitino.rel.types.Types.ShortType;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test class for {@link ConvertUtil}. */
public class TestConvertUtil extends TestBaseConvert {
  @Test
  public void testToIcebergSchema() {
    Column[] columns = createColumns("col_1", "col_2", "col_3", "col_4");
    String col5Name = "col_5";
    IcebergColumn col5 =
        IcebergColumn.builder()
            .withName(col5Name)
            .withType(
                com.apache.gravitino.rel.types.Types.MapType.valueNullable(
                    com.apache.gravitino.rel.types.Types.ListType.nullable(
                        com.apache.gravitino.rel.types.Types.TimestampType.withTimeZone()),
                    com.apache.gravitino.rel.types.Types.MapType.valueNullable(
                        com.apache.gravitino.rel.types.Types.StringType.get(),
                        com.apache.gravitino.rel.types.Types.DateType.get())))
            .withComment(TEST_COMMENT)
            .build();
    columns = ArrayUtils.add(columns, col5);
    SortOrder[] sortOrder = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    IcebergTable icebergTable =
        IcebergTable.builder()
            .withName(TEST_NAME)
            .withAuditInfo(
                AuditInfo.builder().withCreator(TEST_NAME).withCreateTime(Instant.now()).build())
            .withProperties(Maps.newHashMap())
            .withSortOrders(sortOrder)
            .withColumns(columns)
            .withComment(TEST_COMMENT)
            .build();
    Schema icebergSchema = ConvertUtil.toIcebergSchema(icebergTable);
    List<Types.NestedField> nestedFields = icebergSchema.columns();
    Assertions.assertEquals(nestedFields.size(), columns.length);

    Map<String, Column> columnByName =
        Arrays.stream(columns).collect(Collectors.toMap(Column::name, v -> v));
    for (Types.NestedField nestedField : nestedFields) {
      Assertions.assertTrue(columnByName.containsKey(nestedField.name()));
      Column column = columnByName.get(nestedField.name());
      Assertions.assertEquals(column.comment(), nestedField.doc());
      checkType(nestedField.type(), column.dataType());
    }

    Types.NestedField col5Field = icebergSchema.findField(col5Name);
    Assertions.assertNotNull(col5Field);
    Assertions.assertFalse(col5Field.type().isStructType());
    Assertions.assertTrue(col5Field.type().isNestedType());
    Assertions.assertTrue(col5Field.type().isMapType());
    Types.MapType mapType = col5Field.type().asMapType();
    Assertions.assertTrue(mapType.isValueOptional());
    Assertions.assertTrue(mapType.keyType().isListType());
    Assertions.assertFalse(mapType.keyType().asListType().elementType().isNestedType());
    Assertions.assertTrue(mapType.keyType().asListType().isElementOptional());
    Assertions.assertTrue(mapType.valueType().isMapType());
    Assertions.assertTrue(mapType.valueType().asMapType().isValueOptional());
    Assertions.assertTrue(mapType.valueType().asMapType().keyType().isPrimitiveType());
    Assertions.assertTrue(mapType.valueType().asMapType().valueType().isPrimitiveType());
  }

  @Test
  public void testToPrimitiveType() {
    ByteType byteType = ByteType.get();
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> CONVERTER.fromGravitino(byteType));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Iceberg do not support Byte and Short Type, use Integer instead"));

    ShortType shortType = ShortType.get();
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> CONVERTER.fromGravitino(shortType));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Iceberg do not support Byte and Short Type, use Integer instead"));

    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.BooleanType.get())
            instanceof Types.BooleanType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.StringType.get())
            instanceof Types.StringType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.IntegerType.get())
            instanceof Types.IntegerType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.LongType.get())
            instanceof Types.LongType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.FloatType.get())
            instanceof Types.FloatType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.DoubleType.get())
            instanceof Types.DoubleType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.DateType.get())
            instanceof Types.DateType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.TimeType.get())
            instanceof Types.TimeType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.BinaryType.get())
            instanceof Types.BinaryType);
    Assertions.assertTrue(
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.UUIDType.get())
            instanceof Types.UUIDType);

    Type timestampTZ =
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.TimestampType.withTimeZone());
    Assertions.assertTrue(timestampTZ instanceof Types.TimestampType);
    Assertions.assertTrue(((Types.TimestampType) timestampTZ).shouldAdjustToUTC());

    Type timestamp =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.TimestampType.withoutTimeZone());
    Assertions.assertTrue(timestamp instanceof Types.TimestampType);
    Assertions.assertFalse(((Types.TimestampType) timestamp).shouldAdjustToUTC());

    Type decimalType =
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.DecimalType.of(9, 2));
    Assertions.assertTrue(decimalType instanceof Types.DecimalType);
    Assertions.assertEquals(9, ((Types.DecimalType) decimalType).precision());
    Assertions.assertEquals(2, ((Types.DecimalType) decimalType).scale());

    Type fixedCharType =
        CONVERTER.fromGravitino(com.apache.gravitino.rel.types.Types.FixedType.of(9));
    Assertions.assertTrue(fixedCharType instanceof Types.FixedType);
    Assertions.assertEquals(9, ((Types.FixedType) fixedCharType).length());

    com.apache.gravitino.rel.types.Type mapType =
        com.apache.gravitino.rel.types.Types.MapType.of(
            com.apache.gravitino.rel.types.Types.StringType.get(),
            com.apache.gravitino.rel.types.Types.IntegerType.get(),
            true);
    Type convertedMapType = CONVERTER.fromGravitino(mapType);
    Assertions.assertTrue(convertedMapType instanceof Types.MapType);
    Assertions.assertTrue(((Types.MapType) convertedMapType).keyType() instanceof Types.StringType);
    Assertions.assertTrue(
        ((Types.MapType) convertedMapType).valueType() instanceof Types.IntegerType);

    Type listType =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.ListType.of(
                com.apache.gravitino.rel.types.Types.FloatType.get(), true));
    Assertions.assertTrue(listType instanceof Types.ListType);
    Assertions.assertTrue(listType.asListType().elementType() instanceof Types.FloatType);
  }

  @Test
  public void testToNestedType() {
    Type listTypeNullable =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.ListType.of(
                com.apache.gravitino.rel.types.Types.FloatType.get(), true));
    Assertions.assertTrue(listTypeNullable instanceof Types.ListType);
    Assertions.assertTrue(listTypeNullable.asListType().elementType() instanceof Types.FloatType);
    Assertions.assertTrue(listTypeNullable.asListType().isElementOptional());

    Type listTypeNotNull =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.ListType.of(
                com.apache.gravitino.rel.types.Types.FloatType.get(), false));
    Assertions.assertTrue(listTypeNotNull instanceof Types.ListType);
    Assertions.assertTrue(listTypeNotNull.asListType().elementType() instanceof Types.FloatType);
    Assertions.assertTrue(listTypeNotNull.asListType().isElementRequired());

    Type mapTypeNullable =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.MapType.of(
                com.apache.gravitino.rel.types.Types.StringType.get(),
                com.apache.gravitino.rel.types.Types.IntegerType.get(),
                true));
    Assertions.assertTrue(mapTypeNullable instanceof Types.MapType);
    Assertions.assertTrue(mapTypeNullable.asMapType().keyType() instanceof Types.StringType);
    Assertions.assertTrue(mapTypeNullable.asMapType().valueType() instanceof Types.IntegerType);
    Assertions.assertTrue(mapTypeNullable.asMapType().isValueOptional());

    Type mapTypeNotNull =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.MapType.of(
                com.apache.gravitino.rel.types.Types.StringType.get(),
                com.apache.gravitino.rel.types.Types.IntegerType.get(),
                false));
    Assertions.assertTrue(mapTypeNotNull instanceof Types.MapType);
    Assertions.assertTrue(mapTypeNotNull.asMapType().keyType() instanceof Types.StringType);
    Assertions.assertTrue(mapTypeNotNull.asMapType().valueType() instanceof Types.IntegerType);
    Assertions.assertTrue(mapTypeNotNull.asMapType().isValueRequired());

    Type structTypeNullable =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.StructType.of(
                com.apache.gravitino.rel.types.Types.StructType.Field.nullableField(
                    "col1",
                    com.apache.gravitino.rel.types.Types.MapType.of(
                        com.apache.gravitino.rel.types.Types.StringType.get(),
                        com.apache.gravitino.rel.types.Types.IntegerType.get(),
                        true)),
                com.apache.gravitino.rel.types.Types.StructType.Field.nullableField(
                    "col2",
                    com.apache.gravitino.rel.types.Types.ListType.of(
                        com.apache.gravitino.rel.types.Types.FloatType.get(), true))));

    Assertions.assertTrue(structTypeNullable instanceof Types.StructType);
    structTypeNullable.asStructType().fields().forEach(f -> Assertions.assertTrue(f.isOptional()));
    Assertions.assertTrue(
        structTypeNullable.asStructType().fields().get(0).type() instanceof Types.MapType);
    Assertions.assertTrue(
        structTypeNullable.asStructType().fields().get(0).type().asMapType().isValueOptional());
    Assertions.assertTrue(
        structTypeNullable.asStructType().fields().get(1).type() instanceof Types.ListType);
    Assertions.assertTrue(
        structTypeNullable.asStructType().fields().get(1).type().asListType().isElementOptional());

    Type structTypeNotNull =
        CONVERTER.fromGravitino(
            com.apache.gravitino.rel.types.Types.StructType.of(
                com.apache.gravitino.rel.types.Types.StructType.Field.notNullField(
                    "col1",
                    com.apache.gravitino.rel.types.Types.MapType.of(
                        com.apache.gravitino.rel.types.Types.StringType.get(),
                        com.apache.gravitino.rel.types.Types.IntegerType.get(),
                        false)),
                com.apache.gravitino.rel.types.Types.StructType.Field.notNullField(
                    "col2",
                    com.apache.gravitino.rel.types.Types.ListType.of(
                        com.apache.gravitino.rel.types.Types.FloatType.get(), false))));
    Assertions.assertTrue(structTypeNotNull instanceof Types.StructType);
    structTypeNotNull.asStructType().fields().forEach(f -> Assertions.assertTrue(f.isRequired()));
    Assertions.assertTrue(
        structTypeNotNull.asStructType().fields().get(0).type() instanceof Types.MapType);
    Assertions.assertTrue(
        structTypeNotNull.asStructType().fields().get(0).type().asMapType().isValueRequired());
    Assertions.assertTrue(
        structTypeNotNull.asStructType().fields().get(1).type() instanceof Types.ListType);
    Assertions.assertTrue(
        structTypeNotNull.asStructType().fields().get(1).type().asListType().isElementRequired());
  }

  @Test
  public void testFormIcebergType() {
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.StringType.get())
            instanceof com.apache.gravitino.rel.types.Types.StringType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.BinaryType.get())
            instanceof com.apache.gravitino.rel.types.Types.BinaryType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.TimeType.get())
            instanceof com.apache.gravitino.rel.types.Types.TimeType);
    com.apache.gravitino.rel.types.Type TimestampTypeWithoutZone =
        CONVERTER.toGravitino(Types.TimestampType.withoutZone());
    Assertions.assertTrue(
        TimestampTypeWithoutZone instanceof com.apache.gravitino.rel.types.Types.TimestampType);
    Assertions.assertFalse(
        ((com.apache.gravitino.rel.types.Types.TimestampType) TimestampTypeWithoutZone)
            .hasTimeZone());
    com.apache.gravitino.rel.types.Type TimestampTypeWithZone =
        CONVERTER.toGravitino(Types.TimestampType.withZone());
    Assertions.assertTrue(
        TimestampTypeWithZone instanceof com.apache.gravitino.rel.types.Types.TimestampType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.TimestampType) TimestampTypeWithZone).hasTimeZone());
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.DoubleType.get())
            instanceof com.apache.gravitino.rel.types.Types.DoubleType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.FloatType.get())
            instanceof com.apache.gravitino.rel.types.Types.FloatType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.IntegerType.get())
            instanceof com.apache.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.LongType.get())
            instanceof com.apache.gravitino.rel.types.Types.LongType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.UUIDType.get())
            instanceof com.apache.gravitino.rel.types.Types.UUIDType);
    Assertions.assertTrue(
        CONVERTER.toGravitino(Types.DateType.get())
            instanceof com.apache.gravitino.rel.types.Types.DateType);

    com.apache.gravitino.rel.types.Type decimalType =
        CONVERTER.toGravitino(Types.DecimalType.of(9, 2));
    Assertions.assertTrue(decimalType instanceof com.apache.gravitino.rel.types.Types.DecimalType);
    Assertions.assertEquals(
        9, ((com.apache.gravitino.rel.types.Types.DecimalType) decimalType).precision());
    Assertions.assertEquals(
        2, ((com.apache.gravitino.rel.types.Types.DecimalType) decimalType).scale());

    com.apache.gravitino.rel.types.Type fixedType =
        CONVERTER.toGravitino(Types.FixedType.ofLength(2));
    Assertions.assertTrue(fixedType instanceof com.apache.gravitino.rel.types.Types.FixedType);
    Assertions.assertEquals(
        2, ((com.apache.gravitino.rel.types.Types.FixedType) fixedType).length());

    Types.MapType mapType =
        Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.IntegerType.get());
    com.apache.gravitino.rel.types.Type gravitinoMapType = CONVERTER.toGravitino(mapType);
    Assertions.assertTrue(gravitinoMapType instanceof com.apache.gravitino.rel.types.Types.MapType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.MapType) gravitinoMapType).keyType()
            instanceof com.apache.gravitino.rel.types.Types.StringType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.MapType) gravitinoMapType).valueType()
            instanceof com.apache.gravitino.rel.types.Types.IntegerType);

    Types.ListType listType = Types.ListType.ofOptional(1, Types.StringType.get());
    com.apache.gravitino.rel.types.Type gravitinoListType = CONVERTER.toGravitino(listType);
    Assertions.assertTrue(
        gravitinoListType instanceof com.apache.gravitino.rel.types.Types.ListType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.ListType) gravitinoListType).elementType()
            instanceof com.apache.gravitino.rel.types.Types.StringType);

    Types.StructType structTypeInside =
        Types.StructType.of(
            Types.NestedField.optional(
                2, "integer_type_inside", Types.IntegerType.get(), "integer type"),
            Types.NestedField.optional(
                3, "string_type_inside", Types.StringType.get(), "string type"));
    Types.StructType structType =
        Types.StructType.of(
            Types.NestedField.optional(0, "integer_type", Types.IntegerType.get(), "integer type"),
            Types.NestedField.optional(1, "struct_type", structTypeInside, "struct type inside"));
    com.apache.gravitino.rel.types.Type gravitinoStructType = CONVERTER.toGravitino(structType);
    // check for type
    Assertions.assertTrue(
        (gravitinoStructType) instanceof com.apache.gravitino.rel.types.Types.StructType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType).fields()[0].type()
            instanceof com.apache.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.StructType)
                    ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                        .fields()[1].type())
                .fields()[0].type()
            instanceof com.apache.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        ((com.apache.gravitino.rel.types.Types.StructType)
                    ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                        .fields()[1].type())
                .fields()[1].type()
            instanceof com.apache.gravitino.rel.types.Types.StringType);
    // check for name
    Assertions.assertEquals(
        structType.fields().get(0).name(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType).fields()[0].name());
    Assertions.assertEquals(
        structType.fields().get(1).name(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType).fields()[1].name());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).name(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].name());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).name(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[1].name());
    // check for comment
    Assertions.assertEquals(
        structType.fields().get(0).doc(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[0].comment());
    Assertions.assertEquals(
        structType.fields().get(1).doc(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[1].comment());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).doc(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].comment());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).doc(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[1].comment());
    // check for nullable
    Assertions.assertEquals(
        structType.fields().get(0).isOptional(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[0].nullable());
    Assertions.assertEquals(
        structType.fields().get(1).isOptional(),
        ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[1].nullable());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).isOptional(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].nullable());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).isOptional(),
        ((com.apache.gravitino.rel.types.Types.StructType)
                ((com.apache.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[1].nullable());
  }

  @Test
  public void testFromNestedField() {
    String colName = RandomStringUtils.randomAlphabetic(10);
    String doc = RandomStringUtils.randomAlphabetic(20);
    Types.NestedField colField =
        Types.NestedField.optional(1, colName, Types.IntegerType.get(), doc);
    IcebergColumn icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertTrue(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.apache.gravitino.rel.types.Types.IntegerType);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.StringType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.apache.gravitino.rel.types.Types.StringType);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.DateType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.apache.gravitino.rel.types.Types.DateType);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField =
        Types.NestedField.required(
            1,
            colName,
            Types.MapType.ofOptional(
                1,
                2,
                Types.ListType.ofOptional(3, Types.StringType.get()),
                Types.DecimalType.of(13, 1)),
            doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.apache.gravitino.rel.types.Types.MapType);
  }

  private static void checkType(Type type, com.apache.gravitino.rel.types.Type expected) {
    com.apache.gravitino.rel.types.Type actual = CONVERTER.toGravitino(type);
    checkType(actual, expected);
  }

  private static void checkType(
      com.apache.gravitino.rel.types.Type actual, com.apache.gravitino.rel.types.Type expected) {
    if (actual instanceof com.apache.gravitino.rel.types.Types.MapType) {
      Assertions.assertTrue(expected instanceof com.apache.gravitino.rel.types.Types.MapType);
      checkType(
          ((com.apache.gravitino.rel.types.Types.MapType) actual).keyType(),
          ((com.apache.gravitino.rel.types.Types.MapType) expected).keyType());
      checkType(
          ((com.apache.gravitino.rel.types.Types.MapType) actual).valueType(),
          ((com.apache.gravitino.rel.types.Types.MapType) expected).valueType());
    } else if (actual instanceof com.apache.gravitino.rel.types.Types.ListType) {
      Assertions.assertTrue(expected instanceof com.apache.gravitino.rel.types.Types.ListType);
      checkType(
          ((com.apache.gravitino.rel.types.Types.ListType) actual).elementType(),
          ((com.apache.gravitino.rel.types.Types.ListType) expected).elementType());
    } else {
      Assertions.assertEquals(expected.getClass(), actual.getClass());
    }
  }
}
