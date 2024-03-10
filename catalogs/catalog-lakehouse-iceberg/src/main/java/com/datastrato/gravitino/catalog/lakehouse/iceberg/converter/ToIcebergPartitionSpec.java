/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.google.common.base.Preconditions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/** Convert Gravitino Transforms to Iceberg PartitionSpec. */
public class ToIcebergPartitionSpec {

  private static final String DOT = ".";

  /**
   * Convert iceberg table to iceberg partition spec through gravitino.
   *
   * @param icebergTable the iceberg table.
   * @return a PartitionSpec
   */
  public static PartitionSpec toPartitionSpec(IcebergTable icebergTable) {
    Schema schema = ConvertUtil.toIcebergSchema(icebergTable);
    return ToIcebergPartitionSpec.toPartitionSpec(schema, icebergTable.partitioning());
  }

  /**
   * Converts gravitino transforms into a {@link PartitionSpec}.
   *
   * @param schema the table schema
   * @param partitioning Gravitino Transforms
   * @return a PartitionSpec
   */
  public static PartitionSpec toPartitionSpec(Schema schema, Transform[] partitioning) {
    if (partitioning == null || partitioning.length == 0) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitioning) {
      if (transform instanceof Transforms.IdentityTransform) {
        String[] fieldName = ((Transforms.IdentityTransform) transform).fieldName();
        String colName = String.join(DOT, fieldName);
        builder.identity(colName);
      } else if (transform instanceof Transforms.BucketTransform) {
        String[][] fieldNames = ((Transforms.BucketTransform) transform).fieldNames();
        Preconditions.checkArgument(
            fieldNames.length == 1, "Iceberg partition does not support multi fields", transform);
        builder.bucket(
            String.join(DOT, fieldNames[0]), ((Transforms.BucketTransform) transform).numBuckets());
      } else if (transform instanceof Transforms.TruncateTransform) {
        Transforms.TruncateTransform truncateTransform = (Transforms.TruncateTransform) transform;
        builder.truncate(
            String.join(DOT, truncateTransform.fieldName()), truncateTransform.width());
      } else if (transform instanceof Transforms.YearTransform) {
        builder.year(String.join(DOT, ((Transforms.YearTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.MonthTransform) {
        builder.month(String.join(DOT, ((Transforms.MonthTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.DayTransform) {
        builder.day(String.join(DOT, ((Transforms.DayTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.HourTransform) {
        builder.hour(String.join(DOT, ((Transforms.HourTransform) transform).fieldName()));
      } else {
        throw new UnsupportedOperationException("Transform is not supported: " + transform.name());
      }
    }
    return builder.build();
  }

  private ToIcebergPartitionSpec() {}
}
