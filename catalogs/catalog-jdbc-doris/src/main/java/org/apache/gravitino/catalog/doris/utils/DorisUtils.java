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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
      Pattern.compile("PARTITION BY \\b(LIST|RANGE)\\b\\((.+)\\)");
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
            .map(entry -> "\"" + entry.getKey() + "\"=\"" + entry.getValue() + "\"")
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
          final String key = matcherProperties.group(1).trim();
          String value = matcherProperties.group(2).trim();
          properties.put(key, value);
        }
      }
    }
    return properties;
  }

  public static Optional<Transform> extractPartitionInfoFromSql(String createTableSql) {
    try {
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
}
