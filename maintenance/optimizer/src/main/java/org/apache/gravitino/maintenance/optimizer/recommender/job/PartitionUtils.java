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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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

  // Predicates use Iceberg Spark system-function forms so rewrite_data_files can translate them
  // via SparkV2Filters (Iceberg 1.4+ / 1.11). Spark builtins such as day()/year() are not used.
  // Identity: "columnName = value"
  // Bucket: "bucket(numBuckets, columnName) = value"
  // Truncate: "truncate(width, columnName) = value"
  // Temporal: "years|months|hours(columnName) = <int>"; days uses DATE literal because the Iceberg
  // Spark days() UDF returns DateType (int epoch-day compares fail with DATATYPE_MISMATCH).
  // Literals: strings / DATE / TIMESTAMP / TIME use double quotes (safe inside CALL where =>
  // '...'; single-quoted typed literals are eaten by the CALL string), numbers bare.
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

  /**
   * Builds the left-hand side of a partition predicate using Iceberg Spark system functions.
   *
   * <p>Temporal transforms use plural names ({@code years}/{@code months}/{@code days}/{@code
   * hours}) matching {@code org.apache.iceberg.spark.functions.SparkFunctions}. {@code bucket} and
   * {@code truncate} put the width argument first, matching Iceberg's Spark UDF signatures.
   */
  private static String buildExpression(Transform transform, String columnName) {
    if (transform instanceof Transforms.IdentityTransform) {
      return columnName;
    } else if (transform instanceof Transforms.YearTransform) {
      return "years(" + columnName + ")";
    } else if (transform instanceof Transforms.MonthTransform) {
      return "months(" + columnName + ")";
    } else if (transform instanceof Transforms.DayTransform) {
      return "days(" + columnName + ")";
    } else if (transform instanceof Transforms.HourTransform) {
      return "hours(" + columnName + ")";
    } else if (transform instanceof Transforms.BucketTransform) {
      return "bucket("
          + ((Transforms.BucketTransform) transform).numBuckets()
          + ", "
          + columnName
          + ")";
    } else if (transform instanceof Transforms.TruncateTransform) {
      return "truncate("
          + ((Transforms.TruncateTransform) transform).width()
          + ", "
          + columnName
          + ")";
    }
    throw new IllegalArgumentException("Unsupported transform: " + transform.getClass().getName());
  }

  private static String formatLiteral(Transform transform, Column column, String rawValue) {
    if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.HourTransform
        || transform instanceof Transforms.BucketTransform) {
      return rawValue;
    }

    // Iceberg Spark days() returns DATE; CALL where => '...' also eats single-quoted DATE
    // literals (escapeSqlString doubling still parses as unquoted). Use double-quoted DATE.
    // Epoch-day -> ISO date mirrors Iceberg DateTimeUtil.daysToIsoDate without depending on
    // iceberg-api (ofEpochDay + ISO_LOCAL_DATE == dateFromDays(...).format(ISO_LOCAL_DATE)).
    if (transform instanceof Transforms.DayTransform) {
      String isoDate =
          LocalDate.ofEpochDay(Integer.parseInt(rawValue)).format(DateTimeFormatter.ISO_LOCAL_DATE);
      return "DATE \"" + isoDate + "\"";
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
        // Double quotes: CALL where => '...' eats single-quoted typed literals.
        return "DATE \"" + rawValue + "\"";
      case TIMESTAMP:
        return "TIMESTAMP \"" + rawValue + "\"";
      case TIME:
        return "TIME \"" + rawValue + "\"";
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
