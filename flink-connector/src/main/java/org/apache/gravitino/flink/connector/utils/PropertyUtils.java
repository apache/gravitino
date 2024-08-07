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
import java.util.Map;
import java.util.stream.Collectors;

public class PropertyUtils {

  public static final String HIVE_PREFIX = "hive.";
  public static final String HADOOP_PREFIX = "hadoop.";
  public static final String FS_PREFIX = "fs.";
  public static final String DFS_PREFIX = "dfs.";

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
}
