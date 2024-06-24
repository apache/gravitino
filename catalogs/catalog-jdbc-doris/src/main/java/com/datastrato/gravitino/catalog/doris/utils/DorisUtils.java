/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.utils;

import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class DorisUtils {
  private static final String PARTITION_BY = "PARTITION BY";
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
        if (line.contains(PARTITION_BY)) {
          String partitionInfoString = line.substring(PARTITION_BY.length() + 1);
          int partitionColumnIndex = partitionInfoString.indexOf("(");
          String partitionType =
              partitionInfoString.substring(0, partitionColumnIndex).toUpperCase();
          String[] columns =
              Arrays.stream(
                      partitionInfoString
                          .substring(partitionColumnIndex + 1, partitionInfoString.length() - 1)
                          .split(", "))
                  .map(s -> s.substring(1, s.length() - 1))
                  .toArray(String[]::new);
          if (LIST_PARTITION.equals(partitionType)) {
            String[][] filedNames =
                Arrays.stream(columns).map(s -> new String[] {s}).toArray(String[][]::new);
            return Optional.of(Transforms.list(filedNames));
          } else if (RANGE_PARTITION.equals(partitionType)) {
            return Optional.of(Transforms.range(new String[] {columns[0]}));
          } else {
            throw new RuntimeException("Cannot extract partition type from create table SQL");
          }
        }
      }
      return Optional.empty();
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
