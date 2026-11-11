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
import java.time.format.DateTimeParseException;
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

/**
 * Builds Iceberg Spark {@code rewrite_data_files} {@code where} predicates from partition paths.
 *
 * <p>Expressions must match Iceberg Spark system functions ({@code years}/{@code months}/{@code
 * days}/{@code hours}/{@code bucket}/{@code truncate}) so {@code SparkV2Filters} can push them
 * down. Spark builtins such as {@code day()} / {@code year()} are intentionally not used.
 *
 * <p>String and typed literals ({@code DATE}/{@code TIMESTAMP}/{@code TIME}) use double quotes
 * because the job embeds the clause in {@code CALL ... where => '...'}—single-quoted typed literals
 * are eaten by that outer string even when SQL-escaped.
 */
public class PartitionUtils {

  /**
   * Builds an OR of per-partition predicates, each wrapped in parentheses.
   *
   * <p>Example: {@code (a = "x" AND b = 1) OR (a = "y" AND b = 2)}.
   */
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
    return predicates.stream().map(p -> "(" + p + ")").collect(Collectors.joining(" OR "));
  }

  /**
   * Builds a single-partition AND predicate.
   *
   * <ul>
   *   <li>Identity: {@code column = literal}
   *   <li>Bucket / truncate: Iceberg argument order ({@code bucket(N, col)}, {@code truncate(W,
   *       col)})
   *   <li>{@code years}/{@code months}/{@code hours}: compare to the stored Iceberg int (years
   *       since 1970, etc.)—not a calendar year/month
   *   <li>{@code days}: Iceberg Spark {@code days()} returns {@code DateType}, so the RHS is {@code
   *       DATE "yyyy-MM-dd"} (ISO from update-stats or epoch-day from metadata)
   * </ul>
   */
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
      // Nested paths like event.ts are absent from schema; fall back to top-level event.
      Column column = columnMap.get(columnName.toLowerCase(Locale.ROOT));
      if (column == null && columnName.indexOf('.') >= 0) {
        column =
            columnMap.get(
                columnName.substring(0, columnName.indexOf('.')).toLowerCase(Locale.ROOT));
      }
      Preconditions.checkNotNull(column, "Column '%s' not found in table schema", columnName);

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
    // years/months/hours/bucket partition values are already Iceberg ints; leave unchanged.
    if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.HourTransform
        || transform instanceof Transforms.BucketTransform) {
      return rawValue;
    }

    // days() → DateType; double-quoted DATE survives CALL where => '...' embedding.
    if (transform instanceof Transforms.DayTransform) {
      return "DATE \"" + toIsoDateFromDayPartitionValue(rawValue) + "\"";
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
      case TIMESTAMP:
      case TIME:
        // Same CALL embedding constraint as days(); see class Javadoc.
        return type.name() + " \"" + rawValue + "\"";
      default:
        return "\"" + escapeStringLiteral(rawValue) + "\"";
    }
  }

  /**
   * Converts a day-partition stored value to {@code yyyy-MM-dd}.
   *
   * <p>Update-stats persists Spark {@code date} partition fields via {@code String.valueOf}, which
   * yields ISO dates (for example {@code 2026-01-01}). Iceberg metadata may also expose the raw
   * epoch-day integer. Accept both; reject anything else with a clear error.
   */
  private static String toIsoDateFromDayPartitionValue(String rawValue) {
    try {
      return LocalDate.parse(rawValue, DateTimeFormatter.ISO_LOCAL_DATE)
          .format(DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (DateTimeParseException ignored) {
      // fall through to epoch-day
    }
    try {
      // Mirrors Iceberg DateTimeUtil.daysToIsoDate without depending on iceberg-api.
      return LocalDate.ofEpochDay(Integer.parseInt(rawValue))
          .format(DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Day partition value must be an ISO date (yyyy-MM-dd) or epoch-day integer, but got: "
              + rawValue,
          e);
    }
  }

  private static String escapeStringLiteral(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String joinFieldName(String[] fieldNameParts) {
    return String.join(".", fieldNameParts);
  }
}
