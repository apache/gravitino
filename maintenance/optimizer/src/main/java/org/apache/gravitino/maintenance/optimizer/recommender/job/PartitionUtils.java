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
import java.math.BigDecimal;
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
import org.apache.gravitino.rel.types.Types;

/**
 * Builds Iceberg Spark {@code rewrite_data_files} {@code where} predicates from partition paths.
 *
 * <p>Expressions must match Iceberg Spark system functions ({@code years}/{@code months}/{@code
 * days}/{@code hours}/{@code bucket}/{@code truncate}) so {@code SparkV2Filters} can push them
 * down. Spark builtins such as {@code day()} / {@code year()} are intentionally not used.
 *
 * <p>String and typed literals ({@code DATE}/{@code TIMESTAMP}/{@code TIME}) use single quotes so
 * predicates stay valid when {@code spark.sql.ansi.doubleQuotedIdentifiers} is enabled (double
 * quotes are identifiers there). The job embeds the clause in {@code CALL ... where => '...'} via
 * {@code IcebergJobUtils.escapeSparkCallStringArg} ({@code '} → {@code \'}), which preserves these
 * inner literals through the outer Spark parse.
 *
 * <p>Non-identity transforms are qualified as {@code catalog.system.years|months|days|hours|...} so
 * resolution does not depend on {@code spark.sql.defaultCatalog} or a catalog {@code
 * default-namespace} (which would otherwise turn bare {@code years(ts)} into {@code
 * catalog.db.years}).
 */
public class PartitionUtils {

  /**
   * Builds an OR of per-partition predicates, each wrapped in parentheses.
   *
   * <p>Example: {@code (a = 'x' AND b = 1) OR (a = 'y' AND b = 2)}.
   *
   * @param catalogName Iceberg Spark catalog used to qualify system functions
   */
  public static String getWhereClauseForPartitions(
      List<PartitionPath> partitions,
      Column[] columns,
      Transform[] partitioning,
      String catalogName) {
    Preconditions.checkArgument(
        partitions != null && !partitions.isEmpty(), "partitions cannot be null or empty");
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(columns), "columns cannot be null or empty");
    Preconditions.checkArgument(
        partitioning != null && partitioning.length > 0, "partitioning cannot be null or empty");

    List<String> predicates = Lists.newArrayListWithExpectedSize(partitions.size());
    for (PartitionPath partition : partitions) {
      predicates.add(
          getWhereClauseForPartition(partition.entries(), columns, partitioning, catalogName));
    }
    return predicates.stream().map(p -> "(" + p + ")").collect(Collectors.joining(" OR "));
  }

  /**
   * Builds a single-partition AND predicate.
   *
   * <ul>
   *   <li>Identity: {@code column = literal}
   *   <li>Bucket / truncate: Iceberg argument order ({@code catalog.system.bucket(N, col)}, {@code
   *       catalog.system.truncate(W, col)})
   *   <li>{@code years}/{@code months}/{@code hours}: compare to the stored Iceberg int (years
   *       since 1970, etc.)—not a calendar year/month
   *   <li>{@code days}: Iceberg Spark {@code days()} returns {@code DateType}, so the RHS is {@code
   *       DATE 'yyyy-MM-dd'}. Stored partition strings must be ISO {@code yyyy-MM-dd} (same as
   *       Iceberg {@code DateTimeUtil.isoDateToDays})
   * </ul>
   *
   * @param catalogName Iceberg Spark catalog used to qualify system functions
   */
  public static String getWhereClauseForPartition(
      List<PartitionEntry> partition,
      Column[] columns,
      Transform[] partitioning,
      String catalogName) {
    Preconditions.checkArgument(
        partition != null && !partition.isEmpty(), "partition cannot be null or empty");
    Preconditions.checkArgument(ArrayUtils.isNotEmpty(columns), "columns cannot be null or empty");
    Preconditions.checkArgument(
        partitioning != null && partitioning.length == partition.size(),
        "partitioning must match the size of partition entries");
    Preconditions.checkArgument(
        catalogName != null && !catalogName.trim().isEmpty(),
        "catalogName cannot be null or empty");

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

      String expression = buildExpression(transform, columnName, catalogName.trim());
      // Walk into struct fields so nested identity uses TIMESTAMP/INT/... not STRUCT→string.
      String literal =
          formatLiteral(transform, resolveLeafDataType(column, columnName), entry.partitionValue());
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

  /** Resolves the leaf type for {@code event.ts}-style paths under a top-level struct column. */
  private static Type resolveLeafDataType(Column column, String columnName) {
    // Exact match (including odd top-level names that contain '.') — no nested walk.
    if (column.name().equalsIgnoreCase(columnName) || columnName.indexOf('.') < 0) {
      return column.dataType();
    }
    Type type = column.dataType();
    String[] parts = columnName.split("\\.");
    for (int i = 1; i < parts.length; i++) {
      if (!(type instanceof Types.StructType)) {
        throw new IllegalArgumentException(
            "Cannot resolve nested field '" + columnName + "': not a struct path");
      }
      Types.StructType.Field matched = null;
      for (Types.StructType.Field field : ((Types.StructType) type).fields()) {
        if (field.name().equalsIgnoreCase(parts[i])) {
          matched = field;
          break;
        }
      }
      if (matched == null) {
        throw new IllegalArgumentException(
            "Nested field '" + columnName + "' not found in table schema");
      }
      type = matched.type();
    }
    return type;
  }

  /**
   * Builds the left-hand side of a partition predicate using Iceberg Spark system functions.
   *
   * <p>Temporal transforms use plural names ({@code years}/{@code months}/{@code days}/{@code
   * hours}) matching {@code org.apache.iceberg.spark.functions.SparkFunctions}. {@code bucket} and
   * {@code truncate} put the width argument first, matching Iceberg's Spark UDF signatures.
   * Non-identity functions are qualified as {@code catalog.system.<fn>} so a catalog {@code
   * default-namespace} cannot rewrite them to {@code catalog.<ns>.<fn>}.
   */
  private static String buildExpression(
      Transform transform, String columnName, String catalogName) {
    if (transform instanceof Transforms.IdentityTransform) {
      return columnName;
    }
    String systemFn = quoteCatalogIdentifier(catalogName) + ".system.";
    if (transform instanceof Transforms.YearTransform) {
      return systemFn + "years(" + columnName + ")";
    } else if (transform instanceof Transforms.MonthTransform) {
      return systemFn + "months(" + columnName + ")";
    } else if (transform instanceof Transforms.DayTransform) {
      return systemFn + "days(" + columnName + ")";
    } else if (transform instanceof Transforms.HourTransform) {
      return systemFn + "hours(" + columnName + ")";
    } else if (transform instanceof Transforms.BucketTransform) {
      return systemFn
          + "bucket("
          + ((Transforms.BucketTransform) transform).numBuckets()
          + ", "
          + columnName
          + ")";
    } else if (transform instanceof Transforms.TruncateTransform) {
      return systemFn
          + "truncate("
          + ((Transforms.TruncateTransform) transform).width()
          + ", "
          + columnName
          + ")";
    }
    throw new IllegalArgumentException("Unsupported transform: " + transform.getClass().getName());
  }

  /** Backtick-quotes a Spark catalog identifier (internal backticks are doubled). */
  private static String quoteCatalogIdentifier(String catalogName) {
    return "`" + catalogName.replace("`", "``") + "`";
  }

  private static String formatLiteral(Transform transform, Type type, String rawValue) {
    Preconditions.checkArgument(rawValue != null, "Partition value cannot be null");

    // years/months/hours/bucket: raw Iceberg ints from producers (e.g. update-stats
    // String.valueOf(Spark partition field)). years = years since 1970 (calendar 2024 → 54),
    // not a calendar year/month; months/hours likewise. Validate before emitting.
    if (transform instanceof Transforms.YearTransform
        || transform instanceof Transforms.MonthTransform
        || transform instanceof Transforms.HourTransform
        || transform instanceof Transforms.BucketTransform) {
      // Same trim + parse as identity integrals so " 54 " does not fail only on one path.
      return formatIntegralLiteral(
          rawValue, "Partition value for years/months/hours/bucket must be an integer");
    }

    // days() → DateType; single-quoted DATE survives ANSI doubleQuotedIdentifiers.
    if (transform instanceof Transforms.DayTransform) {
      return "DATE '" + escapeStringLiteral(toIsoDateFromDayPartitionValue(rawValue)) + "'";
    }

    switch (type.name()) {
      case BOOLEAN:
        return formatBooleanLiteral(rawValue);
      case BYTE:
      case SHORT:
      case INTEGER:
      case LONG:
        return formatIntegralLiteral(
            rawValue, "Partition value for identity integral column must be an integer");
      case FLOAT:
      case DOUBLE:
        return formatFloatingLiteral(rawValue);
      case DECIMAL:
        return formatDecimalLiteral(rawValue);
      case DATE:
      case TIMESTAMP:
      case TIME:
        return type.name() + " '" + escapeStringLiteral(rawValue) + "'";
      default:
        return "'" + escapeStringLiteral(rawValue) + "'";
    }
  }

  /**
   * Parses identity boolean partition values. Only {@code true}/{@code false} (any case) are
   * accepted so malformed imports cannot inject SQL.
   */
  private static String formatBooleanLiteral(String rawValue) {
    String normalized = rawValue.trim().toLowerCase(Locale.ROOT);
    if (!"true".equals(normalized) && !"false".equals(normalized)) {
      throw new IllegalArgumentException(
          "Partition value for boolean must be true or false, but got: " + rawValue);
    }
    return normalized;
  }

  /**
   * Parses integral partition values so inputs like {@code 7 OR 1 = 1} fail instead of broadening
   * the predicate.
   *
   * @param messagePrefix context-specific prefix before {@code , but got: ...}
   */
  private static String formatIntegralLiteral(String rawValue, String messagePrefix) {
    try {
      return Long.toString(Long.parseLong(rawValue.trim()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(messagePrefix + ", but got: " + rawValue, e);
    }
  }

  /**
   * Parses floating partition values and emits a canonical numeric literal.
   *
   * <p>{@link Double#parseDouble(String)} accepts Java hex floats such as {@code 0X1P0}, but Spark
   * SQL tokenizes that spelling as an identifier. Re-emit via {@link Double#toString(double)} so
   * the predicate always uses a numeric literal Spark can parse.
   */
  private static String formatFloatingLiteral(String rawValue) {
    try {
      double parsed = Double.parseDouble(rawValue.trim());
      if (Double.isInfinite(parsed) || Double.isNaN(parsed)) {
        throw new NumberFormatException("non-finite");
      }
      return Double.toString(parsed);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Partition value for floating type must be a finite number, but got: " + rawValue, e);
    }
  }

  private static String formatDecimalLiteral(String rawValue) {
    String trimmed = rawValue.trim();
    try {
      new BigDecimal(trimmed);
      return trimmed;
    } catch (NumberFormatException | ArithmeticException e) {
      throw new IllegalArgumentException(
          "Partition value for decimal must be a number, but got: " + rawValue, e);
    }
  }

  /**
   * Validates and normalizes a day-partition stored value to {@code yyyy-MM-dd}.
   *
   * <p>Only ISO {@code yyyy-MM-dd} is accepted, matching Iceberg {@code
   * DateTimeUtil.isoDateToDays(String)}. TMS update-stats stores Spark {@code DateType} partition
   * values via {@code String.valueOf}, which is already ISO. Epoch-day ints and compact forms like
   * {@code 20260101} are rejected rather than guessed.
   */
  private static String toIsoDateFromDayPartitionValue(String rawValue) {
    try {
      return LocalDate.parse(rawValue, DateTimeFormatter.ISO_LOCAL_DATE)
          .format(DateTimeFormatter.ISO_LOCAL_DATE);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Day partition value must be an ISO date (yyyy-MM-dd), but got: " + rawValue, e);
    }
  }

  /**
   * Escapes a value for a single-quoted Iceberg Spark literal ({@code \} → {@code \\}, {@code '} →
   * {@code \'}).
   *
   * <p>Does not apply CALL {@code where => '...'} outer escaping; the job builder doubles
   * backslashes (and re-encodes {@code '}) when embedding this clause.
   */
  private static String escapeStringLiteral(String value) {
    return value.replace("\\", "\\\\").replace("'", "\\'");
  }

  private static String joinFieldName(String[] fieldNameParts) {
    return String.join(".", fieldNameParts);
  }
}
