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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.sql.SparkSession;

/**
 * Shared utility methods for Iceberg maintenance jobs.
 *
 * <p>Provides SQL escaping, argument parsing, Spark configuration utilities, and classpath checks
 * used by built-in Iceberg Spark jobs.
 */
public final class IcebergJobUtils {

  /** Shared CLI flag name (without leading dashes) for Iceberg catalog. */
  public static final String OPTION_CATALOG = "catalog";

  /** Shared CLI flag name (without leading dashes) for table identifier. */
  public static final String OPTION_TABLE = "table";

  /** Shared CLI flag name (without leading dashes) for custom Spark configs JSON. */
  public static final String OPTION_SPARK_CONF = "spark-conf";

  private static final String ICEBERG_SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog";

  private IcebergJobUtils() {}

  /**
   * Ensures the Iceberg Spark runtime is on the current classpath.
   *
   * <p>Built-in templates configure {@code IcebergSparkSessionExtensions} and {@code SparkCatalog},
   * but Spark only warns when those classes are missing and continues without Iceberg support. Call
   * this after {@code SparkSession} creation (so {@code spark.jars} from {@code spark_conf} is
   * visible) and fail the job when the runtime is absent. Job {@code main} methods should call
   * {@link #requireIcebergSparkRuntimeOrExit(SparkSession)} instead.
   *
   * @throws IllegalStateException when required Iceberg Spark classes cannot be loaded
   */
  public static void requireIcebergSparkRuntime() {
    requireClass(
        IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS, "Iceberg Spark session extensions");
    requireClass(ICEBERG_SPARK_CATALOG, "Iceberg Spark catalog");
  }

  /**
   * Ensures the Iceberg Spark runtime is on the classpath, or prints the error, stops {@code
   * spark}, and exits the process.
   *
   * @param spark Spark session created for this job; stopped if the runtime check fails
   */
  public static void requireIcebergSparkRuntimeOrExit(SparkSession spark) {
    try {
      requireIcebergSparkRuntime();
    } catch (IllegalStateException e) {
      System.err.println("Error: " + e.getMessage());
      spark.stop();
      System.exit(1);
    }
  }

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
    return new HashMap<>(
        IcebergSparkConfigUtils.parseFlatJsonMap(sparkConfJson, OPTION_SPARK_CONF));
  }

  /** Visible for unit tests that assert the missing-class error message. */
  static void requireClassForTest(String className, String description) {
    requireClass(className, description);
  }

  private static void requireClass(String className, String description) {
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader fallbackLoader = IcebergJobUtils.class.getClassLoader();
    try {
      // initialize=false: only verify the class is loadable; avoid running <clinit> here and
      // misreporting init-time LinkageError as a missing iceberg-spark-runtime jar.
      Class.forName(className, false, contextLoader != null ? contextLoader : fallbackLoader);
    } catch (ClassNotFoundException | LinkageError first) {
      if (contextLoader != null && contextLoader != fallbackLoader) {
        try {
          Class.forName(className, false, fallbackLoader);
          return;
        } catch (ClassNotFoundException | LinkageError ignored) {
          // Fall through to the user-facing error.
        }
      }
      throw new IllegalStateException(
          String.format(
              "Missing %s (%s). Built-in Iceberg jobs need iceberg-spark-runtime on the Spark "
                  + "classpath (for example via spark.jars in spark_conf, or installed into the "
                  + "Spark environment). A stock Spark distribution does not include it. Match "
                  + "the artifact to your Spark, Scala, and Iceberg versions.",
              description, className),
          first);
    }
  }
}
