/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg.converter;

import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_BUCKET;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_TRUNCATE;

import com.datastrato.graviton.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.substrait.expression.ImmutableExpression;
import java.util.Locale;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/** Convert Graviton Transforms to Iceberg PartitionSpec. */
public class ToIcebergPartitionSpec {

  private static final String DOT = ".";

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
        String colName = String.join(DOT, fieldName);
        builder.identity(colName);
      } else if (transform instanceof Transforms.FunctionTrans) {
        String colName =
            String.join(DOT, ((Transforms.NamedReference) transform.arguments()[0]).value());
        switch (transform.name().toLowerCase(Locale.ROOT)) {
          case NAME_OF_BUCKET:
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
          case NAME_OF_TRUNCATE:
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
    Transform expr = transform.arguments()[1];
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
