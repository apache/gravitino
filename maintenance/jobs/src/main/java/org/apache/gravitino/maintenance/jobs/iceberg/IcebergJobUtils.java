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
package org.apache.gravitino.maintenance.jobs.iceberg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

/**
 * Shared utility methods for Iceberg maintenance jobs.
 *
 * <p>Provides SQL escaping, argument parsing, and Spark configuration utilities used by both {@link
 * IcebergRewriteDataFilesJob} and {@link IcebergExpireSnapshotsJob}.
 */
public final class IcebergJobUtils {

  private IcebergJobUtils() {}

  /**
   * Escape single quotes in SQL string literals by replacing ' with ''.
   *
   * @param value the string value to escape
   * @return escaped string safe for use in SQL string literals
   */
  public static String escapeSqlString(String value) {
    if (value == null) {
      return null;
    }
    return value.replace("'", "''");
  }

  /**
   * Escape and quote a SQL identifier with backticks.
   *
   * <p>Internal backticks are doubled to prevent breaking out of the quoted identifier. The result
   * is wrapped in backticks so that identifiers containing special characters (whitespace, dots,
   * semicolons) are treated as a single identifier token.
   *
   * @param identifier the SQL identifier to escape and quote
   * @return backtick-quoted identifier safe for use in SQL, or null if input is null
   */
  public static String escapeSqlIdentifier(String identifier) {
    if (identifier == null) {
      return null;
    }
    String escaped = identifier.replace("`", "``");
    return "`" + escaped + "`";
  }

  /**
   * Parse command line arguments in --key value format.
   *
   * <p>Supports boolean flags (--flag without a value) by storing them with a "true" value.
   *
   * @param args command line arguments
   * @return map of argument names to values
   */
  public static Map<String, String> parseArguments(String[] args) {
    Map<String, String> argMap = new HashMap<>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2); // Remove "--" prefix

        // Check if there's a value for this key (not another flag)
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          String value = args[i + 1];
          // Only add non-empty values
          if (value != null && !value.trim().isEmpty()) {
            argMap.put(key, value);
          }
          i++; // Skip the value in next iteration
        } else {
          // Boolean flag with no value - treat as "true"
          argMap.put(key, "true");
        }
      }
    }

    return argMap;
  }

  /**
   * Parse custom Spark configurations from JSON string.
   *
   * @param sparkConfJson JSON string containing Spark configurations
   * @return map of Spark configuration keys to values
   * @throws IllegalArgumentException if JSON parsing fails
   */
  public static Map<String, String> parseCustomSparkConfigs(String sparkConfJson) {
    if (sparkConfJson == null || sparkConfJson.isEmpty()) {
      return new HashMap<>();
    }

    try {
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> parsedMap =
          mapper.readValue(sparkConfJson, new TypeReference<Map<String, Object>>() {});

      Map<String, String> configs = new HashMap<>();
      for (Map.Entry<String, Object> entry : parsedMap.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();
        configs.put(key, value == null ? "" : value.toString());
      }
      return configs;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse Spark configurations JSON: "
              + sparkConfJson
              + ". Error: "
              + e.getMessage(),
          e);
    }
  }
}
