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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Built-in job for expiring old snapshots from Iceberg tables.
 *
 * <p>This job leverages Iceberg's ExpireSnapshots procedure to remove snapshot metadata and
 * associated data files that are no longer needed, preventing unbounded metadata growth.
 */
public class IcebergExpireSnapshotsJob implements BuiltInJob {

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-expire-snapshots";
  private static final String VERSION = "v1";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in Iceberg expire snapshots job template for metadata cleanup")
        .withExecutable(resolveExecutable(IcebergExpireSnapshotsJob.class))
        .withClassName(IcebergExpireSnapshotsJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Main entry point for the expire snapshots job.
   *
   * <p>Uses named arguments for flexibility:
   *
   * <ul>
   *   <li>--catalog &lt;catalog_name&gt; Required. Iceberg catalog name.
   *   <li>--table &lt;table_identifier&gt; Required. Table name (db.table)
   *   <li>--older-than &lt;timestamp&gt; Optional. Expire snapshots older than this timestamp
   *       (e.g., '2024-01-01 00:00:00')
   *   <li>--retain-last &lt;count&gt; Optional. Number of most recent snapshots to retain
   *   <li>--stream-results Optional. Flag to enable streaming of intermediate results
   *   <li>--spark-conf &lt;spark_conf_json&gt; Optional. JSON map of custom Spark configurations
   * </ul>
   *
   * <p><b>Important Notes on Special Characters:</b>
   *
   * <ul>
   *   <li><b>Via Gravitino API:</b> Pass values as-is without shell escaping. Gravitino handles
   *       escaping internally via ProcessBuilder.
   *   <li><b>Via Command Line:</b> Use shell quoting. Example: {@code --older-than '2024-01-01
   *       00:00:00'}
   * </ul>
   *
   * <p>Example via command line: --catalog iceberg_catalog --table db.sample --older-than
   * '2024-01-01 00:00:00' --retain-last 5
   *
   * <p>Example via Gravitino API:
   *
   * <pre>{@code
   * Map<String, String> jobConf = new HashMap<>();
   * jobConf.put("catalog_name", "iceberg_catalog");
   * jobConf.put("table_identifier", "db.sample");
   * jobConf.put("older_than", "2024-01-01 00:00:00");
   * jobConf.put("retain_last", "5");
   * metalake.runJob("builtin-iceberg-expire-snapshots", jobConf);
   * }</pre>
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      printUsage();
      System.exit(1);
    }

    // Parse named arguments
    Map<String, String> argMap = IcebergJobUtils.parseArguments(args);

    // Validate required arguments
    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");

    if (catalogName == null || tableIdentifier == null) {
      System.err.println("Error: --catalog and --table are required arguments");
      printUsage();
      System.exit(1);
    }

    // Optional arguments
    String olderThan = argMap.get("older-than");
    String retainLast = argMap.get("retain-last");
    // --stream-results is a boolean flag (presence = true)
    boolean streamResults = argMap.containsKey("stream-results");
    String sparkConfJson = argMap.get("spark-conf");

    // Validate retain-last if provided
    try {
      validateRetainLast(retainLast);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      printUsage();
      System.exit(1);
    }

    // Build Spark session with custom configs if provided
    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Expire Snapshots");

    // Apply custom Spark configurations if provided
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      try {
        Map<String, String> customConfigs = IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson);
        for (Map.Entry<String, String> entry : customConfigs.entrySet()) {
          sparkBuilder.config(entry.getKey(), entry.getValue());
        }
        System.out.println("Applied custom Spark configurations: " + customConfigs);
      } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        printUsage();
        System.exit(1);
      }
    }

    SparkSession spark = sparkBuilder.getOrCreate();

    try {
      // Build the procedure call SQL
      String sql =
          buildProcedureCall(catalogName, tableIdentifier, olderThan, retainLast, streamResults);

      System.out.println("Executing Iceberg expire_snapshots procedure: " + sql);

      // Execute the procedure
      Row[] results = (Row[]) spark.sql(sql).collect();

      // Print results
      if (results.length > 0) {
        Row result = results[0];
        System.out.printf(
            "Expire Snapshots Results:%n"
                + "  Deleted data files: %d%n"
                + "  Deleted manifest files: %d%n"
                + "  Deleted manifest lists: %d%n",
            result.getLong(0), result.getLong(1), result.getLong(2));
      }

      System.out.println("Expire snapshots job completed successfully");
    } catch (Exception e) {
      System.err.println("Error executing expire snapshots job: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      spark.stop();
    }
  }

  /**
   * Build the SQL CALL statement for the expire_snapshots procedure.
   *
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier Fully qualified table name
   * @param olderThan Timestamp to expire snapshots older than
   * @param retainLast Number of most recent snapshots to retain
   * @param streamResults Whether to stream intermediate results
   * @return SQL CALL statement
   */
  static String buildProcedureCall(
      String catalogName,
      String tableIdentifier,
      String olderThan,
      String retainLast,
      boolean streamResults) {

    StringBuilder sql = new StringBuilder();
    sql.append("CALL ")
        .append(IcebergJobUtils.escapeSqlIdentifier(catalogName))
        .append(".system.expire_snapshots(");
    sql.append("table => '").append(IcebergJobUtils.escapeSqlString(tableIdentifier)).append("'");

    if (olderThan != null && !olderThan.isEmpty()) {
      sql.append(", older_than => TIMESTAMP '")
          .append(IcebergJobUtils.escapeSqlString(olderThan))
          .append("'");
    }

    if (retainLast != null && !retainLast.isEmpty()) {
      sql.append(", retain_last => ").append(Integer.parseInt(retainLast));
    }

    if (streamResults) {
      sql.append(", stream_results => true");
    }

    sql.append(")");
    return sql.toString();
  }

  /**
   * Validate the retain-last parameter value.
   *
   * @param retainLast the retain-last value to validate
   * @throws IllegalArgumentException if the value is invalid
   */
  static void validateRetainLast(String retainLast) {
    if (retainLast == null || retainLast.isEmpty()) {
      return; // retain-last is optional
    }

    try {
      int value = Integer.parseInt(retainLast);
      if (value < 1) {
        throw new IllegalArgumentException(
            "Invalid retain-last value '" + retainLast + "'. Must be a positive integer (>= 1)");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid retain-last value '" + retainLast + "'. Must be a positive integer");
    }
  }

  /** Print usage information. */
  private static void printUsage() {
    System.err.println(
        "Usage: IcebergExpireSnapshotsJob [OPTIONS]\n"
            + "\n"
            + "Required Options:\n"
            + "  --catalog <name>          Iceberg catalog name registered in Spark\n"
            + "  --table <identifier>      Fully qualified table name (e.g., db.table_name)\n"
            + "\n"
            + "Optional Options:\n"
            + "  --older-than <timestamp>  Expire snapshots older than this timestamp\n"
            + "                              Example: '2024-01-01 00:00:00'\n"
            + "                              Default: 5 days ago (Iceberg default)\n"
            + "  --retain-last <count>     Number of most recent snapshots to retain\n"
            + "                              Must be a positive integer (>= 1)\n"
            + "                              Default: 1 (Iceberg default)\n"
            + "  --stream-results          Enable streaming of intermediate delete results\n"
            + "  --spark-conf <json>       JSON map of custom Spark configurations\n"
            + "                              Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'\n"
            + "                              Note: Overriding required catalog/extensions/app-name configs is unsupported\n"
            + "\n"
            + "Examples:\n"
            + "  # Basic expire with defaults (5 days, retain 1)\n"
            + "  --catalog iceberg_prod --table db.sample\n"
            + "\n"
            + "  # Expire snapshots older than a specific date\n"
            + "  --catalog iceberg_prod --table db.sample --older-than '2024-01-01 00:00:00'\n"
            + "\n"
            + "  # Retain the last 5 snapshots\n"
            + "  --catalog iceberg_prod --table db.sample --retain-last 5\n"
            + "\n"
            + "  # Expire with all options and streaming\n"
            + "  --catalog iceberg_prod --table db.sample --older-than '2024-06-01 00:00:00' \\\n"
            + "    --retain-last 3 --stream-results");
  }

  /**
   * Build template arguments list with named argument format.
   *
   * @return list of template arguments
   */
  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--older-than",
        "{{older_than}}",
        "--retain-last",
        "{{retain_last}}",
        "{{stream_results}}",
        "--spark-conf",
        "{{spark_conf}}");
  }

  /**
   * Build Spark configuration template.
   *
   * @return map of Spark configuration keys to template values
   */
  private static Map<String, String> buildSparkConfigs() {
    return IcebergSparkConfigUtils.buildTemplateSparkConfigs();
  }
}
