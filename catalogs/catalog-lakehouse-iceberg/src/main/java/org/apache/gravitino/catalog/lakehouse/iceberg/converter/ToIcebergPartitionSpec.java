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
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import com.google.common.base.Preconditions;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergTable;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

/** Convert Apache Gravitino Transforms to an Apache Iceberg PartitionSpec. */
public class ToIcebergPartitionSpec {

  private static final String DOT = ".";

  /**
   * Convert Iceberg table to Iceberg partition spec through Gravitino.
   *
   * @param icebergTable the Iceberg table.
   * @return a PartitionSpec
   */
  public static PartitionSpec toPartitionSpec(IcebergTable icebergTable) {
    Schema schema = ConvertUtil.toIcebergSchema(icebergTable);
    return ToIcebergPartitionSpec.toPartitionSpec(schema, icebergTable.partitioning());
  }

  /**
   * Converts Gravitino transforms into a {@link PartitionSpec}.
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
