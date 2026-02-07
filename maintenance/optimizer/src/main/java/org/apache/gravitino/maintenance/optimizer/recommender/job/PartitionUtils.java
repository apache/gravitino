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

package org.apache.gravitino.maintenance.optimizer.recommender.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Type;

public class PartitionUtils {

  public static String getWhereClauseForPartitions(
      List<PartitionPath> partitions, Column[] columns, Transform[] partitioning) {
    Preconditions.checkArgument(
        partitions != null && !partitions.isEmpty(), "partitions cannot be null or empty");
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(columns), "columns cannot be null or empty");
    Preconditions.checkArgument(
        partitioning != null && partitioning.length > 0, "partitioning cannot be null or empty");

    List<String> predicates = Lists.newArrayListWithExpectedSize(partitions.size());
    for (PartitionPath partition : partitions) {
      predicates.add(getWhereClauseForPartition(partition.entries(), columns, partitioning));
    }
    // Wrap each partition-level predicate in parentheses to preserve logical grouping
    // e.g. "(col1 = v1 AND col2 = v2) OR (col1 = v3 AND col2 = v4)"
    return predicates.stream().map(p -> "(" + p + ")").collect(Collectors.joining(" OR "));
  }

  // For Identity transform, the where clause is like "columnName = value"
  // For bucket transform, the where clause is like "bucket(columnName, numBuckets) = value"
  // For truncate transform, the where clause is like "truncate(columnName, width) = value"
  // For year/month/day/hour transform, the where clause is like "year(columnName) = value"
  // We could get value from partition.get(i).partitionValue(), if the value type is string, we need
  // to add quotes like "value",  if the value type is number, we don't need to add quotes. if the
  // value type is date/datetime, we need to add quotes like TIMESTAMP '2024-01-01'
  public static String getWhereClauseForPartition(
      List<PartitionEntry> partition, Column[] columns, Transform[] partitioning) {
    Preconditions.checkArgument(
        partition != null && !partition.isEmpty(), "partition cannot be null or empty");
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(columns), "columns cannot be null or empty");
    Preconditions.checkArgument(
        partitioning != null && partitioning.length == partition.size(),
        "partitioning must match the size of partition entries");

    Map<String, Column> columnMap =
        Arrays.stream(columns)
            .collect(
                Collectors.toMap(
                    c -> c.name().toLowerCase(Locale.ROOT), Function.identity(), (l, r) -> l));

    List<String> predicates = Lists.newArrayListWithExpectedSize(partition.size());
    for (int i = 0; i < partition.size(); i++) {
      PartitionEntry entry = partition.get(i);
      Transform transform = partitioning[i];

      String columnName = getColumnName(transform);
      Column column =
          Preconditions.checkNotNull(
              columnMap.get(columnName.toLowerCase(Locale.ROOT)),
              "Column '%s' not found in table schema",
              columnName);

      String expression = buildExpression(transform, columnName);
      String literal = formatLiteral(transform, column, entry.partitionValue());
      predicates.add(String.format("%s = %s", expression, literal));
    }

    return String.join(" AND ", predicates);
  }

  private static String getColumnName(Transform transform) {
    if (transform instanceof Transform.SingleFieldTransform) {
      return joinFieldName(((Transform.SingleFieldTransform) transform).fieldName());
    } else if (transform instanceof Transforms.TruncateTransform) {
      return joinFieldName(((Transforms.TruncateTransform) transform).fieldName());
    } else if (transform instanceof Transforms.BucketTransform) {
      String[][] fieldNames = ((Transforms.BucketTransform) transform).fieldNames();
      Preconditions.checkArgument(
          fieldNames.length > 0, "Bucket transform must have at least one field");
      return joinFieldName(fieldNames[0]);
    }
    throw new IllegalArgumentException("Unsupported transform: " + transform.getClass().getName());
  }

  private static String buildExpression(Transform transform, String columnName) {
    if (transform instanceof Transforms.IdentityTransform) {
      return columnName;
    } else if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.DayTransform
        || transform instanceof Transforms.HourTransform) {
      return transform.name() + "(" + columnName + ")";
    } else if (transform instanceof Transforms.BucketTransform) {
      return "bucket("
          + columnName
          + ", "
          + ((Transforms.BucketTransform) transform).numBuckets()
          + ")";
    } else if (transform instanceof Transforms.TruncateTransform) {
      return "truncate("
          + columnName
          + ", "
          + ((Transforms.TruncateTransform) transform).width()
          + ")";
    }
    throw new IllegalArgumentException("Unsupported transform: " + transform.getClass().getName());
  }

  private static String formatLiteral(Transform transform, Column column, String rawValue) {
    if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.DayTransform
        || transform instanceof Transforms.HourTransform
        || transform instanceof Transforms.BucketTransform) {
      return rawValue;
    }

    Type type = column.dataType();
    switch (type.name()) {
      case BOOLEAN:
        return rawValue.toLowerCase(Locale.ROOT);
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return rawValue;
      case DATE:
        return "DATE '" + rawValue + "'";
      case TIMESTAMP:
        return "TIMESTAMP '" + rawValue + "'";
      case TIME:
        return "TIME '" + rawValue + "'";
      default:
        return "\"" + escapeStringLiteral(rawValue) + "\"";
    }
  }

  private static String escapeStringLiteral(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String joinFieldName(String[] fieldNameParts) {
    return String.join(".", fieldNameParts);
  }
}
