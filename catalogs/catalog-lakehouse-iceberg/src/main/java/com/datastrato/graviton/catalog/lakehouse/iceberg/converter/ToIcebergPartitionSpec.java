/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.substrait.expression.ImmutableExpression;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/** Convert Graviton Transforms to Iceberg PartitionSpec. */
public class ToIcebergPartitionSpec {

  /**
   * Convert iceberg table to iceberg partition spec through graviton.
   *
   * @param icebergTable the iceberg table.
   * @return
   */
  @VisibleForTesting
  public static PartitionSpec toPartitionSpec(IcebergTable icebergTable) {
    Schema schema = ConvertUtil.toIcebergSchema(icebergTable);
    return ToIcebergPartitionSpec.toPartitionSpec(schema, icebergTable.partitioning());
  }

  /**
   * Converts graviton transforms into a {@link PartitionSpec}.
   *
   * @param schema the table schema
   * @param partitioning Graviton Transforms
   * @return a PartitionSpec
   */
  @VisibleForTesting
  public static PartitionSpec toPartitionSpec(Schema schema, Transform[] partitioning) {
    if (partitioning == null || partitioning.length == 0) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitioning) {
      if (transform instanceof Transforms.NamedReference) {
        String[] fieldName = ((Transforms.NamedReference) transform).value();
        Preconditions.checkArgument(
            fieldName.length == 1, "Iceberg partition does not support nested field", transform);
        String colName = Arrays.stream(fieldName).collect(Collectors.joining());
        builder.identity(colName);
      } else if (transform instanceof Transforms.FunctionTrans) {
        Preconditions.checkArgument(
            transform.arguments().length == 1,
            "Iceberg partition does not support nested field",
            transform);
        String colName =
            Arrays.stream(transform.arguments())
                .map(t -> ((Transforms.NamedReference) t).value()[0])
                .collect(Collectors.joining());
        switch (transform.name().toLowerCase(Locale.ROOT)) {
          case "identity":
            builder.identity(colName);
            break;
          case "bucket":
            builder.bucket(colName, findWidth(transform));
            break;
          case Transforms.NAME_OF_YEAR:
            builder.year(colName);
            break;
          case Transforms.NAME_OF_MONTH:
            builder.month(colName);
            break;
          case Transforms.NAME_OF_DAY:
            builder.day(colName);
            break;
          case Transforms.NAME_OF_HOUR:
            builder.hour(colName);
            break;
          case "truncate":
            builder.truncate(colName, findWidth(transform));
            break;
          default:
            throw new UnsupportedOperationException(
                "Transform is not supported: " + transform.name());
        }
      } else {
        throw new UnsupportedOperationException("Transform is not supported: " + transform.name());
      }
    }
    return builder.build();
  }

  private static int findWidth(Transform transform) {
    Preconditions.checkArgument(
        transform.arguments().length == 1, "Transform with multiple arguments is not supported");
    Transform expr = transform.arguments()[0];
    if (expr instanceof Transforms.LiteralReference) {
      Transforms.LiteralReference literalReference = (Transforms.LiteralReference) expr;
      if (literalReference.value() instanceof ImmutableExpression.I8Literal) {
        int val =
            ((ImmutableExpression.I8Literal) ((Transforms.LiteralReference) expr).value()).value();
        checkTransformArgument(val > 0, transform);
        return val;
      } else if (literalReference.value() instanceof ImmutableExpression.I16Literal) {
        int val = ((ImmutableExpression.I16Literal) literalReference.value()).value();
        checkTransformArgument(val > 0, transform);
        return val;
      } else if (literalReference.value() instanceof ImmutableExpression.I32Literal) {
        int val = ((ImmutableExpression.I32Literal) literalReference.value()).value();
        checkTransformArgument(val > 0, transform);
        return val;

      } else if (literalReference.value() instanceof ImmutableExpression.I64Literal) {
        long val = ((ImmutableExpression.I64Literal) literalReference.value()).value();
        checkTransformArgument(val > 0 && val < Integer.MAX_VALUE, transform);
        return Math.toIntExact(val);
      }
    }
    throw new IllegalArgumentException("Cannot find width for transform: " + transform.name());
  }

  private static void checkTransformArgument(boolean val, Transform transform) {
    Preconditions.checkArgument(val, "Unsupported width for transform: %s", transform.name());
  }
}
