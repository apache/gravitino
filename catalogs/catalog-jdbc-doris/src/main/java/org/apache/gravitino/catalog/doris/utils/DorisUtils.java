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
package org.apache.gravitino.catalog.doris.utils;

import static org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils.escapeSqlLiteral;
import static org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils.unescapeSqlLiteral;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Distributions.DistributionImpl;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DorisUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DorisUtils.class);
  private static final Pattern PARTITION_INFO_PATTERN =
      Pattern.compile("PARTITION BY \\b(LIST|RANGE)\\b\\s*\\((.+)\\)");
  private static final Pattern RANGE_PARTITION_START_PATTERN =
      Pattern.compile("(?im)^\\s*(AUTO\\s+)?PARTITION\\s+BY\\s+RANGE\\s*\\(");
  private static final Pattern FUNCTION_EXPRESSION_PATTERN =
      Pattern.compile("(?is)^[\\p{L}_][\\p{L}\\p{N}_$]*\\s*\\(");
  private static final Pattern SIMPLE_IDENTIFIER_PATTERN =
      Pattern.compile("[\\p{L}_][\\p{L}\\p{N}_$]*");
  private static final String DATE_TRUNC_FUNCTION = "date_trunc";
  private static final char BACK_TICK = '`';

  private static final Pattern DISTRIBUTION_INFO_PATTERN =
      Pattern.compile(
          "DISTRIBUTED BY\\s+(HASH|RANDOM)\\s*(\\(([^)]+)\\))?(?:\\s*BUCKETS\\s+(\\d+|AUTO))?",
          Pattern.CASE_INSENSITIVE);

  private static final String LIST_PARTITION = "LIST";
  private static final String RANGE_PARTITION = "RANGE";

  private DorisUtils() {}

  // convert Map<String, String> properties to SQL String
  public static String generatePropertiesSql(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return "";
    }
    StringBuilder sqlBuilder = new StringBuilder(" PROPERTIES (\n");
    sqlBuilder.append(
        properties.entrySet().stream()
            .map(
                entry ->
                    "\""
                        + escapeSqlLiteral(entry.getKey(), '"')
                        + "\"=\""
                        + escapeSqlLiteral(entry.getValue(), '"')
                        + "\"")
            .collect(Collectors.joining(",\n")));
    sqlBuilder.append("\n)");
    return sqlBuilder.toString();
  }

  public static Map<String, String> extractPropertiesFromSql(String createTableSql) {
    Map<String, String> properties = new HashMap<>();
    String[] lines = createTableSql.split("\n");

    boolean isProperties = false;
    final String sProperties = "\"(.*)\"\\s*=\\s*\"(.*)\",?";
    final Pattern patternProperties = Pattern.compile(sProperties);

    for (String line : lines) {
      if (line.contains("PROPERTIES")) {
        isProperties = true;
      }

      if (isProperties) {
        final Matcher matcherProperties = patternProperties.matcher(line);
        if (matcherProperties.find()) {
          // generatePropertiesSql escapes keys and values for double-quoted literals, and SHOW
          // CREATE echoes them escaped; unescape both so callers observe the original text.
          final String key = unescapeSqlLiteral(matcherProperties.group(1).trim(), '"');
          String value = unescapeSqlLiteral(matcherProperties.group(2).trim(), '"');
          properties.put(key, value);
        }
      }
    }
    return properties;
  }

  public static Optional<Transform> extractPartitionInfoFromSql(String createTableSql) {
    try {
      Matcher rangeStartMatcher = RANGE_PARTITION_START_PATTERN.matcher(createTableSql);
      if (rangeStartMatcher.find()) {
        boolean autoPartition = rangeStartMatcher.group(1) != null;
        int openingParenthesis = rangeStartMatcher.end() - 1;
        int closingParenthesis = findMatchingParenthesis(createTableSql, openingParenthesis);
        String rangeExpression =
            closingParenthesis < 0
                ? createTableSql.substring(rangeStartMatcher.end()).trim()
                : createTableSql.substring(openingParenthesis + 1, closingParenthesis).trim();
        if (autoPartition || FUNCTION_EXPRESSION_PATTERN.matcher(rangeExpression).find()) {
          return closingParenthesis < 0
              ? Optional.empty()
              : extractDateTruncTransform(rangeExpression);
        }
      }

      String[] lines = createTableSql.split("\n");
      for (String line : lines) {
        Matcher matcher = PARTITION_INFO_PATTERN.matcher(line.trim());
        if (matcher.matches()) {
          String partitionType = matcher.group(1);
          String partitionInfoString = matcher.group(2);
          String[] columns =
              Arrays.stream(partitionInfoString.split(", "))
                  .map(s -> s.substring(1, s.length() - 1))
                  .toArray(String[]::new);
          if (LIST_PARTITION.equals(partitionType)) {
            String[][] filedNames =
                Arrays.stream(columns).map(s -> new String[] {s}).toArray(String[][]::new);
            return Optional.of(Transforms.list(filedNames));
          } else if (RANGE_PARTITION.equals(partitionType)) {
            return Optional.of(Transforms.range(new String[] {columns[0]}));
          }
        }
      }
      return Optional.empty();
    } catch (Exception e) {
      LOGGER.warn("Failed to extract partition info", e);
      return Optional.empty();
    }
  }

  /**
   * Returns whether the transform has the exact shape supported by Doris AUTO RANGE partitioning.
   *
   * @param transform The transform to check.
   * @return {@code true} if the transform is a single-column {@code date_trunc} transform with a
   *     string literal interval.
   */
  public static boolean isAutoRangeTransform(Transform transform) {
    if (!(transform instanceof Transforms.ApplyTransform)) {
      return false;
    }

    Transforms.ApplyTransform applyTransform = (Transforms.ApplyTransform) transform;
    if (!DATE_TRUNC_FUNCTION.equalsIgnoreCase(applyTransform.name())) {
      return false;
    }

    Expression[] arguments = applyTransform.arguments();
    if (arguments == null
        || arguments.length != 2
        || !(arguments[0] instanceof NamedReference.FieldReference)) {
      return false;
    }

    String[] fieldNames = ((NamedReference.FieldReference) arguments[0]).fieldName();
    return fieldNames != null
        && fieldNames.length == 1
        && fieldNames[0] != null
        && !fieldNames[0].isEmpty()
        && arguments[1] instanceof Literal
        && ((Literal<?>) arguments[1]).value() instanceof String;
  }

  private static Optional<Transform> extractDateTruncTransform(String expression) {
    int openingParenthesis = expression.indexOf('(');
    if (openingParenthesis < 0
        || !DATE_TRUNC_FUNCTION.equalsIgnoreCase(
            expression.substring(0, openingParenthesis).trim())) {
      return Optional.empty();
    }

    int closingParenthesis = findMatchingParenthesis(expression, openingParenthesis);
    if (closingParenthesis != expression.length() - 1) {
      return Optional.empty();
    }

    List<String> arguments =
        splitFunctionArguments(expression, openingParenthesis + 1, closingParenthesis);
    if (arguments.size() != 2) {
      return Optional.empty();
    }

    String columnName = parseColumnReference(arguments.get(0));
    String interval = parseStringLiteral(arguments.get(1));
    if (columnName == null || interval == null) {
      return Optional.empty();
    }

    return Optional.of(
        Transforms.apply(
            DATE_TRUNC_FUNCTION,
            new Expression[] {NamedReference.field(columnName), Literals.stringLiteral(interval)}));
  }

  private static String parseColumnReference(String columnReference) {
    String trimmed = columnReference.trim();
    if (trimmed.length() > 1
        && trimmed.charAt(0) == BACK_TICK
        && trimmed.charAt(trimmed.length() - 1) == BACK_TICK) {
      StringBuilder columnName = new StringBuilder();
      for (int i = 1; i < trimmed.length() - 1; i++) {
        char current = trimmed.charAt(i);
        if (current == BACK_TICK) {
          if (i + 1 >= trimmed.length() - 1 || trimmed.charAt(i + 1) != BACK_TICK) {
            return null;
          }
          columnName.append(current);
          i++;
        } else {
          columnName.append(current);
        }
      }
      return columnName.length() == 0 ? null : columnName.toString();
    }
    return SIMPLE_IDENTIFIER_PATTERN.matcher(trimmed).matches() ? trimmed : null;
  }

  private static String parseStringLiteral(String value) {
    String trimmed = value.trim();
    if (trimmed.length() < 2
        || trimmed.charAt(0) != '\''
        || trimmed.charAt(trimmed.length() - 1) != '\'') {
      return null;
    }
    return unescapeSqlLiteral(trimmed.substring(1, trimmed.length() - 1), '\'');
  }

  private static List<String> splitFunctionArguments(String expression, int start, int end) {
    List<String> arguments = new ArrayList<>();
    char quote = 0;
    int nestedParentheses = 0;
    int argumentStart = start;
    for (int i = start; i < end; i++) {
      char current = expression.charAt(i);
      if (quote != 0) {
        if (current == '\\' && quote != BACK_TICK && i + 1 < end) {
          i++;
        } else if (current == quote) {
          if (i + 1 < end && expression.charAt(i + 1) == quote) {
            i++;
          } else {
            quote = 0;
          }
        }
      } else if (current == '\'' || current == '"' || current == BACK_TICK) {
        quote = current;
      } else if (current == '(') {
        nestedParentheses++;
      } else if (current == ')') {
        if (--nestedParentheses < 0) {
          return List.of();
        }
      } else if (current == ',' && nestedParentheses == 0) {
        arguments.add(expression.substring(argumentStart, i).trim());
        argumentStart = i + 1;
      }
    }
    if (quote != 0 || nestedParentheses != 0) {
      return List.of();
    }
    arguments.add(expression.substring(argumentStart, end).trim());
    return arguments;
  }

  private static int findMatchingParenthesis(String value, int openingParenthesis) {
    char quote = 0;
    int parentheses = 0;
    for (int i = openingParenthesis; i < value.length(); i++) {
      char current = value.charAt(i);
      if (quote != 0) {
        if (current == '\\' && quote != BACK_TICK && i + 1 < value.length()) {
          i++;
        } else if (current == quote) {
          if (i + 1 < value.length() && value.charAt(i + 1) == quote) {
            i++;
          } else {
            quote = 0;
          }
        }
      } else if (current == '\'' || current == '"' || current == BACK_TICK) {
        quote = current;
      } else if (current == '(') {
        parentheses++;
      } else if (current == ')' && --parentheses == 0) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Generate sql fragment that create partition in Apache Doris.
   *
   * <p>The sql fragment looks like "PARTITION {partitionName} VALUES {values}", for example:
   *
   * <pre>PARTITION `p20240724` VALUES LESS THAN ("2024-07-24")</pre>
   *
   * <pre>PARTITION `p20240724_v1` VALUES IN ("2024-07-24", "v1")</pre>
   *
   * @param partition The partition to be created.
   * @return The partition sql fragment.
   */
  public static String generatePartitionSqlFragment(Partition partition) {
    String partitionSqlFragment = "PARTITION `%s` VALUES %s";
    if (partition instanceof RangePartition) {
      return String.format(
          partitionSqlFragment,
          partition.name(),
          generateRangePartitionValues((RangePartition) partition));
    } else if (partition instanceof ListPartition) {
      return String.format(
          partitionSqlFragment,
          partition.name(),
          generateListPartitionSqlValues((ListPartition) partition));
    } else {
      throw new IllegalArgumentException("Unsupported partition type of Doris");
    }
  }

  private static String generateRangePartitionValues(RangePartition rangePartition) {
    Literal<?> upper = rangePartition.upper();
    Literal<?> lower = rangePartition.lower();
    String partitionValues;
    if (Literals.NULL.equals(upper) && Literals.NULL.equals(lower)) {
      partitionValues = "LESS THAN MAXVALUE";
    } else if (Literals.NULL.equals(lower)) {
      partitionValues = String.format("LESS THAN (\"%s\")", upper.value());
    } else if (Literals.NULL.equals(upper)) {
      partitionValues = String.format("[(\"%s\"), (MAXVALUE))", lower.value());
    } else {
      partitionValues = String.format("[(\"%s\"), (\"%s\"))", lower.value(), upper.value());
    }
    return partitionValues;
  }

  private static String generateListPartitionSqlValues(ListPartition listPartition) {
    Literal<?>[][] lists = listPartition.lists();
    ImmutableList.Builder<String> listValues = ImmutableList.builder();
    for (Literal<?>[] part : lists) {
      String values;
      if (part.length > 1) {
        values =
            String.format(
                "(%s)",
                Arrays.stream(part)
                    .map(p -> "\"" + p.value() + "\"")
                    .collect(Collectors.joining(",")));
      } else {
        values = String.format("\"%s\"", part[0].value());
      }
      listValues.add(values);
    }
    return String.format("IN (%s)", listValues.build().stream().collect(Collectors.joining(",")));
  }

  public static Distribution extractDistributionInfoFromSql(String createTableSql) {
    Matcher matcher = DISTRIBUTION_INFO_PATTERN.matcher(createTableSql.trim());
    if (matcher.find()) {
      String distributionType = matcher.group(1);

      // For Random distribution, no need to specify distribution columns.
      String distributionColumns = matcher.group(3);
      String[] columns =
          Objects.equals(distributionColumns, null)
              ? new String[] {}
              : Arrays.stream(distributionColumns.split(","))
                  .map(String::trim)
                  .map(f -> f.substring(1, f.length() - 1))
                  .toArray(String[]::new);

      // Default bucket number is 1, auto is -1.
      int bucketNum = extractBucketNum(matcher);

      return new DistributionImpl.Builder()
          .withStrategy(Strategy.getByName(distributionType))
          .withNumber(bucketNum)
          .withExpressions(
              Arrays.stream(columns)
                  .map(col -> NamedReference.field(new String[] {col}))
                  .toArray(NamedReference[]::new))
          .build();
    }

    throw new RuntimeException("Failed to extract distribution info in sql:" + createTableSql);
  }

  private static int extractBucketNum(Matcher matcher) {
    int bucketNum = 1;
    if (matcher.group(4) != null) {
      String bucketValue = matcher.group(4).trim();
      // Use -1 to indicate auto bucket.
      bucketNum =
          bucketValue.toUpperCase().equals("AUTO")
              ? Distributions.AUTO
              : Integer.valueOf(bucketValue);
    }
    return bucketNum;
  }

  public static String toBucketNumberString(int number) {
    return number == Distributions.AUTO ? "AUTO" : String.valueOf(number);
  }
}
