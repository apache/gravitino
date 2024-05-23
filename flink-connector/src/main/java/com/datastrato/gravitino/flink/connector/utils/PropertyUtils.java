/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.utils;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class PropertyUtils {

  public static final String HIVE_PREFIX = "hive.";
  public static final String HADOOP_PREFIX = "hadoop.";
  public static final String FS_PREFIX = "hive.";
  public static final String DFS_PREFIX = "dfs.";

  public static Map<String, String> getHadoopAndHivePorperties(Map<String, String> properties) {
    if (properties == null) {
      return Collections.emptyMap();
    }

    return properties.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(HADOOP_PREFIX)
                    || entry.getKey().startsWith(FS_PREFIX)
                    || entry.getKey().startsWith(DFS_PREFIX)
                    || entry.getKey().startsWith(HIVE_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
