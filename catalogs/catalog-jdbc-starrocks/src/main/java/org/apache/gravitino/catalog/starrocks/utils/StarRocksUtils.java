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
package org.apache.gravitino.catalog.starrocks.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksUtils.class);

  private static final Pattern PARTITION_INFO_PATTERN =
      Pattern.compile("PARTITION BY \\b(LIST|RANGE)\\b\\((.+)\\)\\s*\\(?");

  private static final Pattern DISTRIBUTION_INFO_PATTERN =
      Pattern.compile(
          "DISTRIBUTED BY\\s+(HASH|RANDOM)\\s*(\\(([^)]+)\\))?\\s*(BUCKETS\\s+(\\d+))?");

  private static final Pattern TABLE_COMMENT_PATTERN =
      Pattern.compile("COMMENT\\s*\"([^\\(]+?)\\s*\\(From Gravitino,.*\\)\"");

  private static final String PARTITION_TYPE_VALUE_PATTERN_STRING =
      "types: \\[([^\\]]+)\\]; keys: \\[([^\\]]+)\\];";
  private static final Pattern PARTITION_TYPE_VALUE_PATTERN =
      Pattern.compile(PARTITION_TYPE_VALUE_PATTERN_STRING);

  private static final Pattern PARTITION_LIST_PATTERN = Pattern.compile("\\[([^\\[\\]]+)\\]");

  private static final Pattern PARTITION_LIST_PATTERN2 = Pattern.compile("\\(([^()]+)\\)");

  private static final String LIST_PARTITION = "LIST";
  private static final String RANGE_PARTITION = "RANGE";
  public static final String ID = "PartitionId";
  public static final String NAME = "PartitionName";
  public static final String KEY = "PartitionKey";
  public static final String VALUES_LIST = "List";
  public static final String VALUES_RANGE = "Range";
  public static final String VISIBLE_VERSION = "VisibleVersion";
  public static final String VISIBLE_VERSION_TIME = "VisibleVersionTime";
  public static final String STATE = "State";
  public static final String DATA_SIZE = "DataSize";
  public static final String IS_IN_MEMORY = "IsInMemory";

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
              Arrays.stream(partitionInfoString.split(","))
                  .map(String::trim)
                  .map(s -> s.replace("`", ""))
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

      int bucketNum = extractBucketNum(matcher);

      return new Distributions.DistributionImpl.Builder()
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

  public static String extractTableCommentFromSql(String createTableSql) {
    Matcher matcher = TABLE_COMMENT_PATTERN.matcher(createTableSql.trim());
    if (matcher.find()) {
      return matcher.group(1);
    }
    return "";
  }

  /**
   * Generate sql fragment that create partition in StarRocks.
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
      throw new IllegalArgumentException("Unsupported partition type of StarRocks");
    }
  }

  private static String generateRangePartitionValues(RangePartition rangePartition) {
    Literal<?> upper = rangePartition.upper();
    String partitionValues;
    if (Literals.NULL.equals(upper)) {
      partitionValues = "LESS THAN MAXVALUE";
    } else {
      partitionValues = String.format("LESS THAN (\"%s\")", upper.value());
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

  private static int extractBucketNum(Matcher matcher) {
    int bucketNum = Distributions.AUTO;
    if (matcher.find(5)) {
      String bucketValue = matcher.group(5);
      if (bucketValue == null) {
        return bucketNum;
      }
      // Use -1 to indicate auto bucket.
      bucketNum = Integer.parseInt(bucketValue);
    }
    return bucketNum;
  }

  public static String toBucketNumberString(int number) {
    return String.valueOf(number);
  }

  public static Partition fromStarRocksPartition(
      String tableName, ResultSet resultSet, Transform partitionInfo, Map<String, Type> columnTypes)
      throws SQLException {
    String partitionName = resultSet.getString(NAME);
    String partitionKey = resultSet.getString(KEY);
    String partitionValues;
    if (partitionInfo instanceof Transforms.RangeTransform) {
      partitionValues = resultSet.getString(VALUES_RANGE);
    } else if (partitionInfo instanceof Transforms.ListTransform) {
      partitionValues = resultSet.getString(VALUES_LIST);
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a partitioned table", tableName));
    }
    ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    propertiesBuilder.put(ID, resultSet.getString(ID));
    propertiesBuilder.put(VISIBLE_VERSION, resultSet.getString(VISIBLE_VERSION));
    propertiesBuilder.put(VISIBLE_VERSION_TIME, resultSet.getString(VISIBLE_VERSION_TIME));
    propertiesBuilder.put(STATE, resultSet.getString(STATE));
    propertiesBuilder.put(KEY, partitionKey);
    propertiesBuilder.put(DATA_SIZE, resultSet.getString(DATA_SIZE));
    propertiesBuilder.put(IS_IN_MEMORY, resultSet.getString(IS_IN_MEMORY));
    ImmutableMap<String, String> properties = propertiesBuilder.build();

    String[] partitionKeys = partitionKey.split(",");
    if (partitionInfo instanceof Transforms.RangeTransform) {
      if (partitionKeys.length != 1) {
        throw new UnsupportedOperationException(
            "Multi-column range partitioning in StarRocks is not supported yet");
      }
      Type partitionColumnType = columnTypes.get(partitionKeys[0].trim());
      Literal<?> lower = Literals.NULL;
      Literal<?> upper = Literals.NULL;
      Matcher matcher = PARTITION_TYPE_VALUE_PATTERN.matcher(partitionValues);
      if (matcher.find()) {
        String lowerValue = matcher.group(2);
        lower = Literals.of(lowerValue, partitionColumnType);
        if (matcher.find()) {
          String upperValue = matcher.group(2);
          upper = Literals.of(upperValue, partitionColumnType);
        }
      }
      return Partitions.range(partitionName, upper, lower, properties);
    } else if (partitionInfo instanceof Transforms.ListTransform) {
      ImmutableList.Builder<Literal<?>[]> lists = ImmutableList.builder();
      partitionValues = partitionValues.trim();
      if (partitionValues.startsWith("(") && partitionValues.endsWith(")")) {
        Matcher matcher = PARTITION_LIST_PATTERN2.matcher(partitionValues);
        while (matcher.find()) {
          String[] values = matcher.group(1).split(",");
          ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
          for (int i = 0; i < values.length; i++) {
            Type partitionColumnType = columnTypes.get(partitionKeys[i].trim());
            literValues.add(
                Literals.of(
                    values[i].trim().replace("\"", "").replace("'", ""), partitionColumnType));
          }
          lists.add(literValues.build().toArray(new Literal<?>[0]));
        }
        return Partitions.list(
            partitionName, lists.build().toArray(new Literal<?>[0][0]), properties);
      } else if (partitionValues.startsWith("[") && partitionValues.endsWith("]")) {
        Matcher matcher = PARTITION_LIST_PATTERN.matcher(partitionValues);
        while (matcher.find()) {
          String[] values = matcher.group(1).split(",");
          ImmutableList.Builder<Literal<?>> literValues = ImmutableList.builder();
          for (int i = 0; i < values.length; i++) {
            Type partitionColumnType = columnTypes.get(partitionKeys[i].trim());
            literValues.add(Literals.of(values[i].replace("\"", ""), partitionColumnType));
          }
          lists.add(literValues.build().toArray(new Literal<?>[0]));
        }
        return Partitions.list(
            partitionName, lists.build().toArray(new Literal<?>[0][0]), properties);
      }
      throw new UnsupportedOperationException(
          String.format("%s is not a partitioned table", tableName));
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a partitioned table", tableName));
    }
  }
}
