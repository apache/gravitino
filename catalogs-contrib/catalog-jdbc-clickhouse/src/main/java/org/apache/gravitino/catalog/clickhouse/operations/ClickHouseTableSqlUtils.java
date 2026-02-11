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
package org.apache.gravitino.catalog.clickhouse.operations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;

final class ClickHouseTableSqlUtils {

  private static final Pattern TO_DATE_PATTERN =
      Pattern.compile("toDate\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_YEAR_PATTERN =
      Pattern.compile("toYear\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern TO_MONTH_PATTERN =
      Pattern.compile("toYYYYMM\\((.+)\\)", Pattern.CASE_INSENSITIVE);
  private static final Pattern FUNCTION_WRAPPER_PATTERN =
      Pattern.compile("^\\s*([A-Za-z0-9_]+)\\((.*)\\)\\s*$");

  private ClickHouseTableSqlUtils() {}

  static Transform[] parsePartitioning(String partitionKey) {
    if (StringUtils.isBlank(partitionKey)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    String trimmedKey = normalizePartitionKey(partitionKey);
    if (StringUtils.isBlank(trimmedKey)) {
      return Transforms.EMPTY_TRANSFORM;
    }

    String[] parts = trimmedKey.split(",");
    List<Transform> transforms = new ArrayList<>();
    for (String part : parts) {
      String expression = StringUtils.trim(part);
      if (StringUtils.isBlank(expression)) {
        continue;
      }
      transforms.add(parsePartitionExpression(expression, partitionKey));
    }

    return transforms.toArray(new Transform[0]);
  }

  static String toPartitionExpression(Transform transform) {
    Preconditions.checkArgument(transform != null, "Partition transform cannot be null");
    Preconditions.checkArgument(
        StringUtils.equalsIgnoreCase(transform.name(), Transforms.NAME_OF_IDENTITY),
        "Unsupported partition transform: " + transform.name());
    Preconditions.checkArgument(
        transform.arguments().length == 1
            && transform.arguments()[0] instanceof NamedReference
            && ((NamedReference) transform.arguments()[0]).fieldName().length == 1,
        "ClickHouse only supports single column identity partitioning");

    String fieldName = ((NamedReference) transform.arguments()[0]).fieldName()[0];
    return quoteIdentifier(fieldName);
  }

  static List<String> extractShardingKeyColumns(String shardingKey) {
    String normalized = normalizeIndexExpression(shardingKey);
    if (StringUtils.isBlank(normalized)) {
      return Collections.emptyList();
    }

    String[] parts = normalized.split(",");
    List<String> columns = new ArrayList<>();
    for (String part : parts) {
      String column = normalizeIdentifier(part);
      Preconditions.checkArgument(
          isSimpleIdentifier(column), "Sharding key contains unsupported expression: %s", part);
      columns.add(column);
    }
    return ImmutableList.copyOf(columns);
  }

  static String formatShardingKey(String shardingKey) {
    String trimmed = StringUtils.trim(shardingKey);
    if (StringUtils.isBlank(trimmed)) {
      return trimmed;
    }

    String normalized = normalizeIdentifier(trimmed);
    if (isSimpleIdentifier(normalized)) {
      return quoteIdentifier(normalized);
    }
    return trimmed;
  }

  static String[][] parseIndexFields(String expression) {
    if (StringUtils.isBlank(expression)) {
      return new String[0][];
    }

    String normalized = normalizeIndexExpression(expression);
    if (StringUtils.isBlank(normalized)) {
      return new String[0][];
    }

    String[] parts = normalized.split(",");
    List<String[]> fields = new ArrayList<>();
    for (String part : parts) {
      String col = normalizeIdentifier(part);
      Preconditions.checkArgument(
          isSimpleIdentifier(col), "Unsupported index expression: " + expression);
      fields.add(new String[] {col});
    }

    return fields.toArray(new String[0][]);
  }

  static String normalizeIndexExpression(String expression) {
    String trimmed = expression.trim();

    boolean stripped = true;
    String current = trimmed;
    while (stripped) {
      stripped = false;
      Matcher matcher = FUNCTION_WRAPPER_PATTERN.matcher(current);
      if (matcher.matches()) {
        current = matcher.group(2).trim();
        stripped = true;
      }
    }

    if (StringUtils.startsWithIgnoreCase(current, "tuple(") && StringUtils.endsWith(current, ")")) {
      current = current.substring("tuple(".length(), current.length() - 1).trim();
    } else if (StringUtils.equalsIgnoreCase(current, "tuple()")) {
      current = "";
    }

    return current;
  }

  static String normalizeIdentifier(String identifier) {
    String col = StringUtils.trim(identifier);
    if (StringUtils.startsWith(col, "`") && StringUtils.endsWith(col, "`") && col.length() >= 2) {
      return col.substring(1, col.length() - 1);
    }
    return col;
  }

  static boolean isSimpleIdentifier(String identifier) {
    return StringUtils.isNotBlank(identifier)
        && !StringUtils.containsAny(identifier, "(", ")", " ", "%", "+", "-", "*", "/");
  }

  private static boolean isStrictIdentifier(String identifier) {
    return StringUtils.isNotBlank(identifier) && identifier.matches("^[a-zA-Z_][a-zA-Z0-9_]*$");
  }

  private static Transform parsePartitionExpression(
      String expression, String originalPartitionKey) {
    String trimmedExpression = StringUtils.trim(expression);

    Matcher toYearMatcher = TO_YEAR_PATTERN.matcher(trimmedExpression);
    if (toYearMatcher.matches()) {
      String identifier = normalizeIdentifier(toYearMatcher.group(1));
      Preconditions.checkArgument(
          StringUtils.isNotBlank(identifier),
          "Unsupported partition expression: " + originalPartitionKey);
      return Transforms.year(identifier);
    }

    Matcher toYYYYMMMatcher = TO_MONTH_PATTERN.matcher(trimmedExpression);
    if (toYYYYMMMatcher.matches()) {
      String identifier = normalizeIdentifier(toYYYYMMMatcher.group(1));
      Preconditions.checkArgument(
          StringUtils.isNotBlank(identifier),
          "Unsupported partition expression: " + originalPartitionKey);
      return Transforms.month(identifier);
    }

    Matcher toDateMatcher = TO_DATE_PATTERN.matcher(trimmedExpression);
    if (toDateMatcher.matches()) {
      String identifier = normalizeIdentifier(toDateMatcher.group(1));
      Preconditions.checkArgument(
          StringUtils.isNotBlank(identifier),
          "Unsupported partition expression: " + originalPartitionKey);
      return Transforms.day(identifier);
    }

    if (trimmedExpression.contains("(") && trimmedExpression.contains(")")) {
      throw new UnsupportedOperationException(
          "Currently Gravitino only supports toYear, toYYYYMM, toDate partition expressions, but got: "
              + trimmedExpression);
    }

    String identifier = normalizeIdentifier(trimmedExpression);
    Preconditions.checkArgument(
        isStrictIdentifier(identifier),
        "Only simple identifier is supported for partition expression, but got: "
            + originalPartitionKey);
    return Transforms.identity(identifier);
  }

  private static String normalizePartitionKey(String partitionKey) {
    String trimmedKey = partitionKey.trim();
    if (StringUtils.equalsIgnoreCase(trimmedKey, "tuple()")) {
      return "";
    }
    if (StringUtils.startsWithIgnoreCase(trimmedKey, "tuple(")
        && StringUtils.endsWith(trimmedKey, ")")) {
      return trimmedKey.substring("tuple(".length(), trimmedKey.length() - 1).trim();
    }
    if (StringUtils.startsWith(trimmedKey, "(") && StringUtils.endsWith(trimmedKey, ")")) {
      return trimmedKey.substring(1, trimmedKey.length() - 1).trim();
    }
    return trimmedKey;
  }

  private static String quoteIdentifier(String identifier) {
    return String.format("`%s`", identifier);
  }
}
