/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.google.common.collect.Maps;
import io.substrait.type.TypeCreator;
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
                TypeCreator.NULLABLE.map(
                    TypeCreator.NULLABLE.list(TypeCreator.NULLABLE.TIMESTAMP_TZ),
                    TypeCreator.NULLABLE.map(
                        TypeCreator.NULLABLE.STRING, TypeCreator.NULLABLE.DATE)))
            .withComment(TEST_COMMENT)
            .build();
    columns = ArrayUtils.add(columns, col5);
    SortOrder[] sortOrder = createSortOrder("col_1", "col_2", "col_3", "col_4", "col_5");
    IcebergTable icebergTable =
        new IcebergTable.Builder()
            .withName(TEST_NAME)
            .withAuditInfo(
                new AuditInfo.Builder()
                    .withCreator(TEST_NAME)
                    .withCreateTime(Instant.now())
                    .build())
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
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.BOOLEAN) instanceof Types.BooleanType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.STRING) instanceof Types.StringType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.I8) instanceof Types.IntegerType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.I16) instanceof Types.IntegerType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.I32) instanceof Types.IntegerType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.I64) instanceof Types.LongType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.FP32) instanceof Types.FloatType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.FP64) instanceof Types.DoubleType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.DATE) instanceof Types.DateType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.TIME) instanceof Types.TimeType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.BINARY) instanceof Types.BinaryType);
    Assertions.assertTrue(
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.UUID) instanceof Types.UUIDType);

    Type timestampTZ = ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.TIMESTAMP_TZ);
    Assertions.assertTrue(timestampTZ instanceof Types.TimestampType);
    Assertions.assertTrue(((Types.TimestampType) timestampTZ).shouldAdjustToUTC());

    Type timestamp = ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.TIMESTAMP);
    Assertions.assertTrue(timestamp instanceof Types.TimestampType);
    Assertions.assertFalse(((Types.TimestampType) timestamp).shouldAdjustToUTC());

    Type decimalType = ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.decimal(9, 2));
    Assertions.assertTrue(decimalType instanceof Types.DecimalType);
    Assertions.assertEquals(((Types.DecimalType) decimalType).precision(), 9);
    Assertions.assertEquals(((Types.DecimalType) decimalType).scale(), 2);

    Type fixedCharType = ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.fixedChar(9));
    Assertions.assertTrue(fixedCharType instanceof Types.FixedType);
    Assertions.assertEquals(((Types.FixedType) fixedCharType).length(), 9);

    io.substrait.type.Type mapType =
        TypeCreator.NULLABLE.map(TypeCreator.NULLABLE.STRING, TypeCreator.NULLABLE.I8);
    Type convertedMapType = ConvertUtil.toIcebergType(true, mapType);
    Assertions.assertTrue(convertedMapType instanceof Types.MapType);
    Assertions.assertTrue(((Types.MapType) convertedMapType).keyType() instanceof Types.StringType);
    Assertions.assertTrue(
        ((Types.MapType) convertedMapType).valueType() instanceof Types.IntegerType);

    Type listType =
        ConvertUtil.toIcebergType(true, TypeCreator.NULLABLE.list(TypeCreator.NULLABLE.FP64));
    Assertions.assertTrue(listType instanceof Types.ListType);
    Assertions.assertTrue(listType.asListType().elementType() instanceof Types.DoubleType);
  }

  @Test
  public void testFormIcebergType() {
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.StringType.get()) instanceof io.substrait.type.Type.Str);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.BinaryType.get())
            instanceof io.substrait.type.Type.Binary);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimeType.get()) instanceof io.substrait.type.Type.Time);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimestampType.withoutZone())
            instanceof io.substrait.type.Type.Timestamp);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.TimestampType.withZone())
            instanceof io.substrait.type.Type.TimestampTZ);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.DoubleType.get()) instanceof io.substrait.type.Type.FP64);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.FloatType.get()) instanceof io.substrait.type.Type.FP32);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.IntegerType.get()) instanceof io.substrait.type.Type.I32);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.LongType.get()) instanceof io.substrait.type.Type.I64);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.UUIDType.get()) instanceof io.substrait.type.Type.UUID);
    Assertions.assertTrue(
        ConvertUtil.formIcebergType(Types.DateType.get()) instanceof io.substrait.type.Type.Date);

    io.substrait.type.Type decimalType = ConvertUtil.formIcebergType(Types.DecimalType.of(9, 2));
    Assertions.assertTrue(decimalType instanceof io.substrait.type.Type.Decimal);
    Assertions.assertEquals(((io.substrait.type.Type.Decimal) decimalType).precision(), 9);
    Assertions.assertEquals(((io.substrait.type.Type.Decimal) decimalType).scale(), 2);

    io.substrait.type.Type fixedType = ConvertUtil.formIcebergType(Types.FixedType.ofLength(2));
    Assertions.assertTrue(fixedType instanceof io.substrait.type.Type.FixedChar);
    Assertions.assertEquals(((io.substrait.type.Type.FixedChar) fixedType).length(), 2);

    Types.MapType mapType =
        Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.IntegerType.get());
    io.substrait.type.Type gravitinoMapType = ConvertUtil.formIcebergType(mapType);
    Assertions.assertTrue(gravitinoMapType instanceof io.substrait.type.Type.Map);
    Assertions.assertTrue(
        ((io.substrait.type.Type.Map) gravitinoMapType).key()
            instanceof io.substrait.type.Type.Str);
    Assertions.assertTrue(
        ((io.substrait.type.Type.Map) gravitinoMapType).value()
            instanceof io.substrait.type.Type.I32);

    Types.ListType listType = Types.ListType.ofOptional(1, Types.StringType.get());
    io.substrait.type.Type gravitinoListType = ConvertUtil.formIcebergType(listType);
    Assertions.assertTrue(gravitinoListType instanceof io.substrait.type.Type.ListType);
    Assertions.assertTrue(
        ((io.substrait.type.Type.ListType) gravitinoListType).elementType()
            instanceof io.substrait.type.Type.Str);
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
    Assertions.assertTrue(icebergColumn.dataType() instanceof io.substrait.type.Type.I32);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.StringType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(icebergColumn.dataType() instanceof io.substrait.type.Type.Str);

    colName = RandomStringUtils.randomAlphabetic(10);
    doc = RandomStringUtils.randomAlphabetic(20);
    colField = Types.NestedField.required(1, colName, Types.DateType.get(), doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(icebergColumn.dataType() instanceof io.substrait.type.Type.Date);

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
                Types.DecimalType.of(1, 13)),
            doc);
    icebergColumn = ConvertUtil.fromNestedField(colField);
    Assertions.assertEquals(icebergColumn.name(), colName);
    Assertions.assertFalse(icebergColumn.nullable());
    Assertions.assertEquals(icebergColumn.comment(), doc);
    Assertions.assertTrue(icebergColumn.dataType() instanceof io.substrait.type.Type.Map);
  }

  private static void checkType(Type type, io.substrait.type.Type expected) {
    io.substrait.type.Type actual = ConvertUtil.formIcebergType(type);
    checkType(actual, expected);
  }

  private static void checkType(io.substrait.type.Type actual, io.substrait.type.Type expected) {
    if (actual instanceof io.substrait.type.Type.Map) {
      Assertions.assertTrue(expected instanceof io.substrait.type.Type.Map);
      checkType(
          ((io.substrait.type.Type.Map) actual).key(),
          ((io.substrait.type.Type.Map) expected).key());
      checkType(
          ((io.substrait.type.Type.Map) actual).value(),
          ((io.substrait.type.Type.Map) expected).value());
    } else if (actual instanceof io.substrait.type.Type.ListType) {
      Assertions.assertTrue(expected instanceof io.substrait.type.Type.ListType);
      checkType(
          ((io.substrait.type.Type.ListType) actual).elementType(),
          ((io.substrait.type.Type.ListType) expected).elementType());
    } else {
      Assertions.assertEquals(expected.getClass(), actual.getClass());
    }
  }
}
