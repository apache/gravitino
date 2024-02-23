/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.types.Types.ByteType;
import com.datastrato.gravitino.rel.types.Types.ShortType;
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
        new IcebergColumn.Builder()
            .withName(col5Name)
            .withType(
                com.datastrato.gravitino.rel.types.Types.MapType.valueNullable(
                    com.datastrato.gravitino.rel.types.Types.ListType.nullable(
                        com.datastrato.gravitino.rel.types.Types.TimestampType.withTimeZone()),
                    com.datastrato.gravitino.rel.types.Types.MapType.valueNullable(
                        com.datastrato.gravitino.rel.types.Types.StringType.get(),
                        com.datastrato.gravitino.rel.types.Types.DateType.get())))
            .withComment(TEST_COMMENT)
            .build();
    columns = ArrayUtils.add(columns, col5);
    SortOrder[] sortOrder = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    IcebergTable icebergTable =
        new IcebergTable.Builder()
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
    Assertions.assertTrue(mapType.keyType().isListType());
    Assertions.assertFalse(mapType.keyType().asListType().elementType().isNestedType());
    Assertions.assertTrue(mapType.valueType().isMapType());
    Assertions.assertTrue(mapType.valueType().asMapType().keyType().isPrimitiveType());
    Assertions.assertTrue(mapType.valueType().asMapType().valueType().isPrimitiveType());
  }

  @Test
  public void testToPrimitiveType() {
    ByteType byteType = ByteType.get();
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> ConvertUtil.toIcebergType(true, byteType));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Iceberg do not support Byte and Short Type, use Integer instead"));

    ShortType shortType = ShortType.get();
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> ConvertUtil.toIcebergType(true, shortType));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Iceberg do not support Byte and Short Type, use Integer instead"));

    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.BooleanType.get())
            instanceof Types.BooleanType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.StringType.get())
            instanceof Types.StringType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.IntegerType.get())
            instanceof Types.IntegerType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.LongType.get())
            instanceof Types.LongType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.FloatType.get())
            instanceof Types.FloatType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.DoubleType.get())
            instanceof Types.DoubleType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.DateType.get())
            instanceof Types.DateType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.TimeType.get())
            instanceof Types.TimeType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.BinaryType.get())
            instanceof Types.BinaryType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.UUIDType.get())
            instanceof Types.UUIDType);

    Type timestampTZ =
        ConvertUtil.toIcebergType(
            true, com.datastrato.gravitino.rel.types.Types.TimestampType.withTimeZone());
    Assertions.assertTrue(timestampTZ instanceof Types.TimestampType);
    Assertions.assertTrue(((Types.TimestampType) timestampTZ).shouldAdjustToUTC());

    Type timestamp =
        ConvertUtil.toIcebergType(
            true, com.datastrato.gravitino.rel.types.Types.TimestampType.withoutTimeZone());
    Assertions.assertTrue(timestamp instanceof Types.TimestampType);
    Assertions.assertFalse(((Types.TimestampType) timestamp).shouldAdjustToUTC());

    Type decimalType =
        ConvertUtil.toIcebergType(
            true, com.datastrato.gravitino.rel.types.Types.DecimalType.of(9, 2));
    Assertions.assertTrue(decimalType instanceof Types.DecimalType);
    Assertions.assertEquals(9, ((Types.DecimalType) decimalType).precision());
    Assertions.assertEquals(2, ((Types.DecimalType) decimalType).scale());

    Type fixedCharType =
        ConvertUtil.toIcebergType(true, com.datastrato.gravitino.rel.types.Types.FixedType.of(9));
    Assertions.assertTrue(fixedCharType instanceof Types.FixedType);
    Assertions.assertEquals(9, ((Types.FixedType) fixedCharType).length());

    com.datastrato.gravitino.rel.types.Type mapType =
        com.datastrato.gravitino.rel.types.Types.MapType.of(
            com.datastrato.gravitino.rel.types.Types.StringType.get(),
            com.datastrato.gravitino.rel.types.Types.IntegerType.get(),
            true);
    Type convertedMapType = ConvertUtil.toIcebergType(true, mapType);
    Assertions.assertTrue(convertedMapType instanceof Types.MapType);
    Assertions.assertTrue(((Types.MapType) convertedMapType).keyType() instanceof Types.StringType);
    Assertions.assertTrue(
        ((Types.MapType) convertedMapType).valueType() instanceof Types.IntegerType);

    Type listType =
        ConvertUtil.toIcebergType(
            true,
            com.datastrato.gravitino.rel.types.Types.ListType.of(
                com.datastrato.gravitino.rel.types.Types.FloatType.get(), true));
    Assertions.assertTrue(listType instanceof Types.ListType);
    Assertions.assertTrue(listType.asListType().elementType() instanceof Types.FloatType);
  }

  @Test
  public void testFormIcebergType() {
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.StringType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.StringType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.BinaryType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.BinaryType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimeType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.TimeType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimestampType.withoutZone())
            instanceof com.datastrato.gravitino.rel.types.Types.TimestampType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimestampType.withZone())
            instanceof com.datastrato.gravitino.rel.types.Types.TimestampType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.DoubleType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.DoubleType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.FloatType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.FloatType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.IntegerType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.LongType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.LongType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.UUIDType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.UUIDType);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.DateType.get())
            instanceof com.datastrato.gravitino.rel.types.Types.DateType);

    com.datastrato.gravitino.rel.types.Type decimalType =
        ConvertUtil.formIcebergType(Types.DecimalType.of(9, 2));
    Assertions.assertTrue(
        decimalType instanceof com.datastrato.gravitino.rel.types.Types.DecimalType);
    Assertions.assertEquals(
        9, ((com.datastrato.gravitino.rel.types.Types.DecimalType) decimalType).precision());
    Assertions.assertEquals(
        2, ((com.datastrato.gravitino.rel.types.Types.DecimalType) decimalType).scale());

    com.datastrato.gravitino.rel.types.Type fixedType =
        ConvertUtil.formIcebergType(Types.FixedType.ofLength(2));
    Assertions.assertTrue(fixedType instanceof com.datastrato.gravitino.rel.types.Types.FixedType);
    Assertions.assertEquals(
        2, ((com.datastrato.gravitino.rel.types.Types.FixedType) fixedType).length());

    Types.MapType mapType =
        Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.IntegerType.get());
    com.datastrato.gravitino.rel.types.Type gravitinoMapType = ConvertUtil.formIcebergType(mapType);
    Assertions.assertTrue(
        gravitinoMapType instanceof com.datastrato.gravitino.rel.types.Types.MapType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.MapType) gravitinoMapType).keyType()
            instanceof com.datastrato.gravitino.rel.types.Types.StringType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.MapType) gravitinoMapType).valueType()
            instanceof com.datastrato.gravitino.rel.types.Types.IntegerType);

    Types.ListType listType = Types.ListType.ofOptional(1, Types.StringType.get());
    com.datastrato.gravitino.rel.types.Type gravitinoListType =
        ConvertUtil.formIcebergType(listType);
    Assertions.assertTrue(
        gravitinoListType instanceof com.datastrato.gravitino.rel.types.Types.ListType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.ListType) gravitinoListType).elementType()
            instanceof com.datastrato.gravitino.rel.types.Types.StringType);

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
    com.datastrato.gravitino.rel.types.Type gravitinoStructType =
        ConvertUtil.formIcebergType(structType);
    // check for type
    Assertions.assertTrue(
        (gravitinoStructType) instanceof com.datastrato.gravitino.rel.types.Types.StructType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                .fields()[0].type()
            instanceof com.datastrato.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                    ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                        .fields()[1].type())
                .fields()[0].type()
            instanceof com.datastrato.gravitino.rel.types.Types.IntegerType);
    Assertions.assertTrue(
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                    ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                        .fields()[1].type())
                .fields()[1].type()
            instanceof com.datastrato.gravitino.rel.types.Types.StringType);
    // check for name
    Assertions.assertEquals(
        structType.fields().get(0).name(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[0].name());
    Assertions.assertEquals(
        structType.fields().get(1).name(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[1].name());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).name(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].name());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).name(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[1].name());
    // check for comment
    Assertions.assertEquals(
        structType.fields().get(0).doc(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[0].comment());
    Assertions.assertEquals(
        structType.fields().get(1).doc(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[1].comment());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).doc(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].comment());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).doc(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[1].comment());
    // check for nullable
    Assertions.assertEquals(
        structType.fields().get(0).isOptional(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[0].nullable());
    Assertions.assertEquals(
        structType.fields().get(1).isOptional(),
        ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
            .fields()[1].nullable());
    Assertions.assertEquals(
        structTypeInside.fields().get(0).isOptional(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
                    .fields()[1].type())
            .fields()[0].nullable());
    Assertions.assertEquals(
        structTypeInside.fields().get(1).isOptional(),
        ((com.datastrato.gravitino.rel.types.Types.StructType)
                ((com.datastrato.gravitino.rel.types.Types.StructType) gravitinoStructType)
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
        icebergColumn.dataType() instanceof com.datastrato.gravitino.rel.types.Types.IntegerType);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.StringType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.datastrato.gravitino.rel.types.Types.StringType);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.DateType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(
        icebergColumn.dataType() instanceof com.datastrato.gravitino.rel.types.Types.DateType);

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
        icebergColumn.dataType() instanceof com.datastrato.gravitino.rel.types.Types.MapType);
  }

  private static void checkType(Type type, com.datastrato.gravitino.rel.types.Type expected) {
    com.datastrato.gravitino.rel.types.Type actual = ConvertUtil.formIcebergType(type);
    checkType(actual, expected);
  }

  private static void checkType(
      com.datastrato.gravitino.rel.types.Type actual,
      com.datastrato.gravitino.rel.types.Type expected) {
    if (actual instanceof com.datastrato.gravitino.rel.types.Types.MapType) {
      Assertions.assertTrue(expected instanceof com.datastrato.gravitino.rel.types.Types.MapType);
      checkType(
          ((com.datastrato.gravitino.rel.types.Types.MapType) actual).keyType(),
          ((com.datastrato.gravitino.rel.types.Types.MapType) expected).keyType());
      checkType(
          ((com.datastrato.gravitino.rel.types.Types.MapType) actual).valueType(),
          ((com.datastrato.gravitino.rel.types.Types.MapType) expected).valueType());
    } else if (actual instanceof com.datastrato.gravitino.rel.types.Types.ListType) {
      Assertions.assertTrue(expected instanceof com.datastrato.gravitino.rel.types.Types.ListType);
      checkType(
          ((com.datastrato.gravitino.rel.types.Types.ListType) actual).elementType(),
          ((com.datastrato.gravitino.rel.types.Types.ListType) expected).elementType());
    } else {
      Assertions.assertEquals(expected.getClass(), actual.getClass());
    }
  }
}
