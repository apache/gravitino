/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergColumn;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.literals.Literal;
import com.datastrato.gravitino.rel.expressions.literals.Literals;
import com.datastrato.gravitino.rel.expressions.sorts.NullOrdering;
import com.datastrato.gravitino.rel.expressions.sorts.SortDirection;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.types.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
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
    GRAVITINO_TYPE.put("BOOLEAN", com.datastrato.gravitino.rel.types.Types.BooleanType.get());
    // Types not supported by iceberg
    //    GRAVITINO_TYPE.put("I8", com.datastrato.gravitino.rel.types.Types.ByteType.get());
    //    GRAVITINO_TYPE.put("I16", com.datastrato.gravitino.rel.types.Types.ShortType.get());
    GRAVITINO_TYPE.put("I32", com.datastrato.gravitino.rel.types.Types.IntegerType.get());
    GRAVITINO_TYPE.put("I64", com.datastrato.gravitino.rel.types.Types.LongType.get());
    GRAVITINO_TYPE.put("FP32", com.datastrato.gravitino.rel.types.Types.FloatType.get());
    GRAVITINO_TYPE.put("FP64", com.datastrato.gravitino.rel.types.Types.DoubleType.get());
    GRAVITINO_TYPE.put("STRING", com.datastrato.gravitino.rel.types.Types.StringType.get());
    GRAVITINO_TYPE.put("BINARY", com.datastrato.gravitino.rel.types.Types.BinaryType.get());
    GRAVITINO_TYPE.put(
        "TIMESTAMP", com.datastrato.gravitino.rel.types.Types.TimestampType.withoutTimeZone());
    GRAVITINO_TYPE.put(
        "TIMESTAMP_TZ", com.datastrato.gravitino.rel.types.Types.TimestampType.withTimeZone());
    GRAVITINO_TYPE.put("DATE", com.datastrato.gravitino.rel.types.Types.DateType.get());
    GRAVITINO_TYPE.put("TIME", com.datastrato.gravitino.rel.types.Types.TimeType.get());
    GRAVITINO_TYPE.put("UUID", com.datastrato.gravitino.rel.types.Types.UUIDType.get());
    // Types not supported by iceberg
    //    GRAVITINO_TYPE.put("INTERVAL_DAY",
    // com.datastrato.gravitino.rel.types.Types.IntervalDayType.get());
    //    GRAVITINO_TYPE.put("INTERVAL_YEAR",
    // com.datastrato.gravitino.rel.types.Types.IntervalYearType.get());

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
          IcebergColumn.builder()
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
          SortOrders.of(
              field(colName),
              RandomUtils.nextBoolean() ? SortDirection.DESCENDING : SortDirection.ASCENDING,
              RandomUtils.nextBoolean() ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST));
    }
    return results.toArray(new SortOrder[0]);
  }

  protected static SortOrder createSortOrder(String name, String colName) {
    return SortOrders.of(
        FunctionExpression.of(name, field(colName)),
        RandomUtils.nextBoolean() ? SortDirection.DESCENDING : SortDirection.ASCENDING,
        RandomUtils.nextBoolean() ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST);
  }

  protected static SortOrder createSortOrder(String name, int width, String colName) {
    return SortOrders.of(
        FunctionExpression.of(name, Literals.integerLiteral(width), field(colName)),
        RandomUtils.nextBoolean() ? SortDirection.DESCENDING : SortDirection.ASCENDING,
        RandomUtils.nextBoolean() ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST);
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

  // Iceberg supports function expressions as SortOrder expressions, the function expression is used
  // to evaluate the input value and return a result.
  // And in Iceberg, these function expressions are represented by
  // `org.apache.iceberg.transforms.Transform`, such as a Bucket(10, column) Transform.
  protected static String getIcebergTransfromString(SortField sortField, Schema schema) {
    String transform = sortField.transform().toString();
    Map<Integer, String> idToName = schema.idToName();
    if (transform.startsWith("year")
        || transform.startsWith("month")
        || transform.startsWith("day")
        || transform.startsWith("hour")
        || transform.startsWith("identity")) {
      return String.format("%s(%s)", transform, idToName.get(sortField.sourceId()));
    } else if (transform.startsWith("truncate") || transform.startsWith("bucket")) {
      return String.format(
          "%s, %s)",
          transform.replace("[", "(").replace("]", ""), idToName.get(sortField.sourceId()));
    } else {
      throw new RuntimeException("Unsupported Iceberg transform type");
    }
  }

  protected static String getGravitinoSortOrderExpressionString(Expression sortOrderExpression) {
    if (sortOrderExpression instanceof NamedReference.FieldReference) {
      NamedReference.FieldReference fieldReference =
          (NamedReference.FieldReference) sortOrderExpression;
      return String.format("identity(%s)", fieldReference.fieldName()[0]);
    } else if (sortOrderExpression instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) sortOrderExpression;
      String functionName = functionExpression.functionName();
      Expression[] arguments = functionExpression.arguments();
      if (arguments.length == 1) {
        return String.format(
            "%s(%s)", functionName, ((NamedReference.FieldReference) arguments[0]).fieldName()[0]);
      } else if (arguments.length == 2) {
        Expression firstArg = arguments[0];
        Preconditions.checkArgument(
            firstArg instanceof Literal
                && ((Literal<?>) firstArg).dataType()
                    instanceof com.datastrato.gravitino.rel.types.Types.IntegerType,
            "The first argument must be a integer literal");
        return String.format(
            "%s(%s, %s)",
            functionName,
            Integer.parseInt(String.valueOf(((Literal<?>) firstArg).value())),
            ((NamedReference.FieldReference) arguments[1]).fieldName()[0]);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Iceberg FunctionExpression in Gravitino should have 1 or 2 arguments, but got %d arguments",
                arguments.length));
      }
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported Gravitino expression type: %s",
              sortOrderExpression.getClass().getName()));
    }
  }

  private static Type getRandomGravitinoType() {
    Collection<Type> values = GRAVITINO_TYPE.values();
    return values.stream()
        .skip(RandomUtils.nextInt(values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }

  private static org.apache.iceberg.types.Type getRandomIcebergType() {
    Collection<org.apache.iceberg.types.Type> values = ICEBERG_TYPE.values();
    return values.stream()
        .skip(RandomUtils.nextInt(values.size()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No type found"));
  }
}
