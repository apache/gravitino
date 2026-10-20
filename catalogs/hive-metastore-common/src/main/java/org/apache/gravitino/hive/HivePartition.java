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
package org.apache.gravitino.hive;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;

/** Represents a Hive identity partition with helpers to build and parse partition specs. */
public final class HivePartition implements IdentityPartition {

  private static final String PARTITION_NAME_DELIMITER = "/";
  private static final String PARTITION_VALUE_DELIMITER = "=";

  // The characters Hive's FileUtils.escapePathName escapes in partition names on non-Windows
  // hosts. Hive classes are not visible to this class in a deployed server, so the rule is copied.
  private static final BitSet CHARS_TO_ESCAPE = new BitSet(128);

  static {
    for (char c = 0; c < ' '; c++) {
      CHARS_TO_ESCAPE.set(c);
    }
    for (char c : "\"#%'*/:=?\\\u007F{[]^".toCharArray()) {
      CHARS_TO_ESCAPE.set(c);
    }
  }

  private final String name;
  private final String[][] fieldNames;
  private final Literal<?>[] values;
  private final Map<String, String> properties;

  private HivePartition(
      String name, String[][] fieldNames, Literal<?>[] values, Map<String, String> properties) {
    Preconditions.checkArgument(fieldNames != null, "Partition field names must not be null");
    Preconditions.checkArgument(values != null, "Partition values must not be null");
    Preconditions.checkArgument(
        fieldNames.length == values.length,
        "Partition field names size %s must equal values size %s",
        fieldNames.length,
        values.length);
    Arrays.stream(fieldNames)
        .forEach(
            fn ->
                Preconditions.checkArgument(
                    fn.length == 1, "Hive catalog does not support nested partition field names"));

    this.fieldNames = fieldNames;
    this.values = values;
    this.properties = properties;
    this.name = StringUtils.isNotEmpty(name) ? name : buildPartitionName();
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(this.name), "Partition name must not be null or empty");
  }

  public static HivePartition identity(String[][] fieldNames, Literal<?>[] values) {
    return identity(fieldNames, values, Collections.emptyMap());
  }

  public static HivePartition identity(
      String[][] fieldNames, Literal<?>[] values, Map<String, String> properties) {
    return new HivePartition(null, fieldNames, values, properties);
  }

  public static HivePartition identity(String partitionName) {
    return identity(partitionName, Collections.emptyMap());
  }

  public static HivePartition identity(String partitionName, Map<String, String> properties) {
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(partitionName), "Partition name must not be null or empty");
    String[][] fieldNames = extractPartitionFieldNames(partitionName);
    Literal<?>[] values =
        extractPartitionValues(partitionName).stream()
            .map(Literals::stringLiteral)
            .toArray(Literal[]::new);
    return new HivePartition(partitionName, fieldNames, values, properties);
  }

  @Override
  public String name() {
    return name;
  }

  public String[][] fieldNames() {
    return fieldNames;
  }

  public Literal<?>[] values() {
    return values;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  private String buildPartitionName() {
    // Escape like HMS does, so the name matches the one HMS and HiveTableConverter return.
    return IntStream.range(0, fieldNames.length)
        .mapToObj(
            idx ->
                escapePathName(fieldNames[idx][0])
                    + PARTITION_VALUE_DELIMITER
                    + escapePathName(String.valueOf(values[idx].value())))
        .collect(Collectors.joining(PARTITION_NAME_DELIMITER));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HivePartition)) {
      return false;
    }
    HivePartition that = (HivePartition) o;
    return Objects.equals(name, that.name)
        && Arrays.deepEquals(fieldNames, that.fieldNames)
        && Arrays.equals(values, that.values)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(name, properties);
    result = 31 * result + Arrays.deepHashCode(fieldNames);
    result = 31 * result + Arrays.hashCode(values);
    return result;
  }

  public static List<String> extractPartitionValues(String partitionName) {
    if (StringUtils.isEmpty(partitionName)) {
      return Collections.emptyList();
    }
    return Arrays.stream(partitionName.split(PARTITION_NAME_DELIMITER))
        .map(
            field -> {
              String[] kv = field.split(PARTITION_VALUE_DELIMITER, 2);
              return kv.length > 1 ? unescapePathName(kv[1]) : "";
            })
        .collect(Collectors.toList());
  }

  public static List<String> extractPartitionValues(
      List<String> partitionFieldNames, String partitionSpec) {
    Preconditions.checkArgument(
        partitionFieldNames != null, "Partition field names must not be null");
    if (partitionFieldNames.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, String> partSpecMap = new HashMap<>();
    if (StringUtils.isNotEmpty(partitionSpec)) {
      Arrays.stream(partitionSpec.split(PARTITION_NAME_DELIMITER))
          .forEach(
              part -> {
                String[] keyValue = part.split(PARTITION_VALUE_DELIMITER, 2);
                if (keyValue.length != 2) {
                  throw new IllegalArgumentException(
                      String.format("Invalid partition format: %s", partitionSpec));
                }
                String fieldName = unescapePathName(keyValue[0]);
                if (!partitionFieldNames.contains(fieldName)) {
                  throw new NoSuchPartitionException(
                      "Hive partition %s does not exist in Hive Metastore", partitionSpec);
                }
                partSpecMap.put(fieldName, unescapePathName(keyValue[1]));
              });
    }

    return partitionFieldNames.stream()
        .map(key -> partSpecMap.getOrDefault(key, ""))
        .collect(Collectors.toList());
  }

  public static String[][] extractPartitionFieldNames(String partitionName) {
    if (StringUtils.isEmpty(partitionName)) {
      return new String[0][0];
    }
    return Arrays.stream(partitionName.split(PARTITION_NAME_DELIMITER))
        .map(part -> new String[] {unescapePathName(part.split(PARTITION_VALUE_DELIMITER, 2)[0])})
        .toArray(String[][]::new);
  }

  /** Escapes a partition column name or value the way Hive's FileUtils.escapePathName does. */
  static String escapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c < CHARS_TO_ESCAPE.size() && CHARS_TO_ESCAPE.get(c)) {
        sb.append('%').append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Reverses {@link #escapePathName(String)} the way Hive's FileUtils.unescapePathName does. */
  static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code;
        try {
          code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
        } catch (NumberFormatException e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
