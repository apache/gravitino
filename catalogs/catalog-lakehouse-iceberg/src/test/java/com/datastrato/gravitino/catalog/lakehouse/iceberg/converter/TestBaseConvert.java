/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SortOrder;
import com.google.common.collect.Lists;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomUtils;
import org.apache.iceberg.types.Types;

/** Provide some basic usage methods and test classes for basic fields. */
public class TestBaseConvert {

  protected static final String TEST_COMMENT = "test_comment";
  protected static final String TEST_NAME = "test";
  protected static final String TEST_FIELD = "test";
  protected static final String TEST_LOCATION = "location";

  protected static final Map<String, Type> GRAVITINO_TYPE = new HashMap<>();
  protected static final Map<String, org.apache.iceberg.types.Type> ICEBERG_TYPE = new HashMap<>();

  static {
    GRAVITINO_TYPE.put("BOOLEAN", TypeCreator.NULLABLE.BOOLEAN);
    // Types not supported by iceberg
    //    GRAVITINO_TYPE.put("I8", TypeCreator.NULLABLE.I8);
    //    GRAVITINO_TYPE.put("I16", TypeCreator.NULLABLE.I16);
    GRAVITINO_TYPE.put("I32", TypeCreator.NULLABLE.I32);
    GRAVITINO_TYPE.put("I64", TypeCreator.NULLABLE.I64);
    GRAVITINO_TYPE.put("FP32", TypeCreator.NULLABLE.FP32);
    GRAVITINO_TYPE.put("FP64", TypeCreator.NULLABLE.FP64);
    GRAVITINO_TYPE.put("STRING", TypeCreator.NULLABLE.STRING);
    GRAVITINO_TYPE.put("BINARY", TypeCreator.NULLABLE.BINARY);
    GRAVITINO_TYPE.put("TIMESTAMP", TypeCreator.NULLABLE.TIMESTAMP);
    GRAVITINO_TYPE.put("TIMESTAMP_TZ", TypeCreator.NULLABLE.TIMESTAMP_TZ);
    GRAVITINO_TYPE.put("DATE", TypeCreator.NULLABLE.DATE);
    GRAVITINO_TYPE.put("TIME", TypeCreator.NULLABLE.TIME);
    GRAVITINO_TYPE.put("UUID", TypeCreator.NULLABLE.UUID);
    // Types not supported by iceberg
    //        ICEBERG_TYPE.put("INTERVAL_DAY", TypeCreator.NULLABLE.INTERVAL_DAY);
    //        ICEBERG_TYPE.put("INTERVAL_YEAR", TypeCreator.NULLABLE.INTERVAL_YEAR);

    ICEBERG_TYPE.put("BOOLEAN", org.apache.iceberg.types.Types.BooleanType.get());
    ICEBERG_TYPE.put("I8", org.apache.iceberg.types.Types.IntegerType.get());
    ICEBERG_TYPE.put("I16", org.apache.iceberg.types.Types.IntegerType.get());
    ICEBERG_TYPE.put("I32", org.apache.iceberg.types.Types.IntegerType.get());
    ICEBERG_TYPE.put("I64", org.apache.iceberg.types.Types.LongType.get());
    ICEBERG_TYPE.put("FP32", org.apache.iceberg.types.Types.FloatType.get());
    ICEBERG_TYPE.put("FP64", org.apache.iceberg.types.Types.DoubleType.get());
    ICEBERG_TYPE.put("STRING", org.apache.iceberg.types.Types.StringType.get());
    ICEBERG_TYPE.put("BINARY", org.apache.iceberg.types.Types.BinaryType.get());
    ICEBERG_TYPE.put("TIMESTAMP", org.apache.iceberg.types.Types.TimestampType.withoutZone());
    ICEBERG_TYPE.put("TIMESTAMP_TZ", org.apache.iceberg.types.Types.TimestampType.withZone());
    ICEBERG_TYPE.put("DATE", org.apache.iceberg.types.Types.DateType.get());
    ICEBERG_TYPE.put("TIME", org.apache.iceberg.types.Types.TimeType.get());
    ICEBERG_TYPE.put("UUID", org.apache.iceberg.types.Types.UUIDType.get());
  }

  protected static Column[] createColumns(String... colNames) {
    ArrayList<Column> results = Lists.newArrayList();
    for (String colName : colNames) {
      results.add(
          new IcebergColumn.Builder()
              .withName(colName)
              .withType(getRandomGravitinoType())
              .withComment(TEST_COMMENT)
              .build());
    }
    return results.toArray(new Column[0]);
  }

  protected static SortOrder[] createSortOrder(String... colNames) {
    ArrayList<SortOrder> results = Lists.newArrayList();
    for (String colName : colNames) {
      results.add(
          SortOrder.fieldSortOrder(
              new String[] {colName},
              RandomUtils.nextBoolean() ? SortOrder.Direction.DESC : SortOrder.Direction.ASC,
              RandomUtils.nextBoolean()
                  ? SortOrder.NullOrdering.FIRST
                  : SortOrder.NullOrdering.LAST));
    }
    return results.toArray(new SortOrder[0]);
  }

  protected static SortOrder createFunctionSortOrder(String name, String colName) {
    return SortOrder.functionSortOrder(
        name,
        new String[] {colName},
        RandomUtils.nextBoolean() ? SortOrder.Direction.DESC : SortOrder.Direction.ASC,
        RandomUtils.nextBoolean() ? SortOrder.NullOrdering.FIRST : SortOrder.NullOrdering.LAST);
  }

  protected static Types.NestedField createNestedField(
      int id, String name, org.apache.iceberg.types.Type type) {
    return Types.NestedField.optional(id, name, type, TEST_COMMENT);
  }

  protected static Types.NestedField[] createNestedField(String... colNames) {
    ArrayList<Types.NestedField> results = Lists.newArrayList();
    for (int i = 0; i < colNames.length; i++) {
      results.add(
          Types.NestedField.of(
              i + 1, RandomUtils.nextBoolean(), colNames[i], getRandomIcebergType(), TEST_COMMENT));
    }
    return results.toArray(new Types.NestedField[0]);
  }

  private static Type getRandomGravitinoType() {
    Collection<Type> values = GRAVITINO_TYPE.values();
    return values.stream()
        .skip(RandomUtils.nextInt(0, values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }

  private static org.apache.iceberg.types.Type getRandomIcebergType() {
    Collection<org.apache.iceberg.types.Type> values = ICEBERG_TYPE.values();
    return values.stream()
        .skip(RandomUtils.nextInt(0, values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }
}
