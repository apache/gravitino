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
package org.apache.gravitino.flink.connector.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;

/** Utility methods for Flink connector properties. */
public class PropertyUtils {

  public static final String HIVE_PREFIX = "hive.";
  public static final String HADOOP_PREFIX = "hadoop.";
  public static final String FS_PREFIX = "fs.";
  public static final String DFS_PREFIX = "dfs.";

  /**
   * Gets Hadoop and Hive properties.
   *
   * @param properties the source properties
   * @return Hadoop and Hive properties
   */
  public static Map<String, String> getHadoopAndHiveProperties(Map<String, String> properties) {
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

  /**
   * Extracts Hadoop configuration from the given options and removes Hadoop-prefixed options.
   *
   * <p>Options with the {@code hadoop.} prefix are written to Hadoop configuration without the
   * prefix. Options with the {@code fs.} or {@code dfs.} prefix are written as-is.
   *
   * @param options mutable options to extract Hadoop configuration from
   * @return Hadoop configuration containing extracted Hadoop options
   */
  public static Configuration extractHadoopConfiguration(Map<String, String> options) {
    Map<String, String> hadoopProps = new HashMap<>();
    options
        .entrySet()
        .removeIf(
            entry -> {
              String hadoopKey = toHadoopConfKey(entry.getKey());
              if (hadoopKey == null) {
                return false;
              }

              hadoopProps.put(hadoopKey, entry.getValue());
              return true;
            });

    Configuration conf = HadoopUtils.getHadoopConfiguration(Options.fromMap(options));
    hadoopProps.forEach(conf::set);
    return conf;
  }

  private static String toHadoopConfKey(String key) {
    if (key.startsWith(HADOOP_PREFIX)) {
      return key.substring(HADOOP_PREFIX.length());
    } else if (key.startsWith(FS_PREFIX) || key.startsWith(DFS_PREFIX)) {
      return key;
    }

    return null;
  }
}
