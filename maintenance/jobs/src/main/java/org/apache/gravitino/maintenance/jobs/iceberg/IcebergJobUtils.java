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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared utility methods for Iceberg maintenance jobs.
 *
 * <p>Provides SQL escaping, argument parsing, Spark configuration utilities, and classpath checks
 * used by built-in Iceberg Spark jobs.
 */
public final class IcebergJobUtils {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergJobUtils.class);

  private static final String ICEBERG_SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog";
  /**
   * Matches a job template placeholder that no job configuration value replaced, e.g. {@code
   * {{use_caching}}}.
   */
  private static final Pattern UNRESOLVED_PLACEHOLDER_PATTERN = Pattern.compile("^\\{\\{[^{}]*}}$");

  /**
   * The Spark configurations that spark-submit decides. Set in the custom Spark configurations of a
   * job, which the driver applies, they would override the submitted value, e.g. run a job in local
   * mode inside the driver pod of a cluster.
   */
  private static final Set<String> SUBMIT_DECIDED_CONFIGS =
      ImmutableSet.of("spark.master", "spark.submit.deployMode");

  /**
   * The Spark configurations that only spark-submit resolves, by the template parameters of the
   * built-in jobs that set them.
   */
  private static final Map<String, String> SUBMIT_TIME_CONFIG_PARAMETERS =
      ImmutableMap.of("spark.jars", "spark_jars", "spark.jars.packages", "spark_jars_packages");

  private IcebergJobUtils() {}

  /**
   * Return the given argument value unless it is an unresolved job template placeholder.
   *
   * <p>Job templates declare optional parameters as {@code {{name}}} placeholders. When the caller
   * omits a parameter, the server leaves the placeholder untouched and it reaches the job as a
   * literal {@code "{{name}}"} argument. Treating that as a real value would forward nonsense to
   * Iceberg, so callers use this method to map it back to "not supplied".
   *
   * @param value the argument value to inspect
   * @return the value, or null if it is null or an unresolved placeholder
   */
  @Nullable
  public static String nullIfUnresolvedPlaceholder(@Nullable String value) {
    if (value == null || UNRESOLVED_PLACEHOLDER_PATTERN.matcher(value.trim()).matches()) {
      return null;
    }
    return value;
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
   * Parse named arguments with explicit values and validate supported and required names.
   *
   * <p>Blank values and unresolved template placeholders are normalized to null. Duplicate names,
   * unknown names, missing values, and absent required arguments are rejected. Unlike the
   * single-argument overload, this parser does not accept bare boolean flags.
   *
   * @param args command line arguments
   * @param supported allowed argument names without the leading dashes
   * @param requiredArguments required argument names without the leading dashes
   * @return map of argument names to normalized values
   * @throws IllegalArgumentException if arguments are invalid
   */
  public static Map<String, String> parseArguments(
      String[] args, Set<String> supported, Set<String> requiredArguments) {
    Map<String, String> parsed = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      String flag = args[i];
      if (flag == null || !flag.startsWith("--") || !supported.contains(flag.substring(2))) {
        throw new IllegalArgumentException("Unknown argument: " + flag);
      }
      if (i + 1 == args.length || args[i + 1] == null || args[i + 1].startsWith("--")) {
        throw new IllegalArgumentException("Missing value for " + flag);
      }
      String key = flag.substring(2);
      if (parsed.containsKey(key)) {
        throw new IllegalArgumentException("Duplicate argument: " + flag);
      }
      String value = nullIfUnresolvedPlaceholder(args[i + 1].trim());
      parsed.put(key, value == null || value.isEmpty() ? null : value);
    }
    for (String required : requiredArguments) {
      if (parsed.get(required) == null) {
        throw new IllegalArgumentException("--" + required + " is required");
      }
    }
    return parsed;
  }

  /**
   * Parse custom Spark configurations from JSON string.
   *
   * @param sparkConfJson JSON string containing Spark configurations
   * @return map of Spark configuration keys to values
   * @throws IllegalArgumentException if JSON parsing fails
   */
  public static Map<String, String> parseCustomSparkConfigs(String sparkConfJson) {
    return filterCustomSparkConfigs(
        IcebergSparkConfigUtils.parseFlatJsonMap(sparkConfJson, "spark-conf"));
  }

  /**
   * Filter the custom Spark configurations of a job, which the driver applies when it creates the
   * SparkSession, against the configurations spark-submit has already set for the driver.
   *
   * <p>{@code spark.master} and {@code spark.submit.deployMode} are dropped if spark-submit has set
   * them, as the job would otherwise run with another master than it was submitted to. A warning is
   * logged for {@code spark.jars} and {@code spark.jars.packages}, which only spark-submit resolves
   * for the driver, and which the job template parameters {@code spark_jars} and {@code
   * spark_jars_packages} set.
   *
   * @param configs the custom Spark configurations
   * @return the custom Spark configurations to apply
   */
  public static Map<String, String> filterCustomSparkConfigs(Map<String, String> configs) {
    return filterCustomSparkConfigs(configs, new SparkConf());
  }

  @VisibleForTesting
  static Map<String, String> filterCustomSparkConfigs(
      Map<String, String> configs, SparkConf submittedConf) {
    Map<String, String> filtered = new LinkedHashMap<>();
    configs.forEach(
        (key, value) -> {
          if (SUBMIT_DECIDED_CONFIGS.contains(key) && submittedConf.contains(key)) {
            LOG.warn(
                "Ignored {}={} of the custom Spark configurations, as the job is submitted with"
                    + " {}={}. Set it when submitting the job instead, for a built-in job by the"
                    + " spark_master parameter.",
                key,
                value,
                key,
                submittedConf.get(key));
            return;
          }
          if (SUBMIT_TIME_CONFIG_PARAMETERS.containsKey(key)) {
            LOG.warn(
                "{} of the custom Spark configurations doesn't add to the classpath of the driver,"
                    + " which spark-submit resolves. Set it by the {} parameter of a built-in job"
                    + " instead.",
                key,
                SUBMIT_TIME_CONFIG_PARAMETERS.get(key));
          }
          filtered.put(key, value);
        });
    return ImmutableMap.copyOf(filtered);
  }

  /**
   * Ensures the Iceberg Spark runtime is on the current classpath.
   *
   * <p>Built-in templates configure {@code IcebergSparkSessionExtensions} and {@code SparkCatalog},
   * but Spark only warns when those classes are missing and continues without Iceberg support. Call
   * this after {@code SparkSession} creation and fail the job when the runtime is absent.
   *
   * @throws IllegalStateException when required Iceberg Spark classes cannot be loaded
   */
  public static void requireIcebergSparkRuntime() {
    requireClass(
        IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS, "Iceberg Spark session extensions");
    requireClass(ICEBERG_SPARK_CATALOG, "Iceberg Spark catalog");
  }

  /**
   * Checks the Iceberg Spark runtime after the session is created, then stops Spark and exits the
   * process on failure.
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
      return;
    }
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
                  + "classpath, for example via the spark_jars or spark_jars_packages parameter "
                  + "of the job, or installed into the Spark environment. spark.jars in "
                  + "spark_conf doesn't add it, as the driver applies spark_conf only after it "
                  + "starts. A stock Spark distribution does not include it. Match the artifact "
                  + "to your Spark, Scala, and Iceberg versions.",
              description, className),
          first);
    }
  }
}
