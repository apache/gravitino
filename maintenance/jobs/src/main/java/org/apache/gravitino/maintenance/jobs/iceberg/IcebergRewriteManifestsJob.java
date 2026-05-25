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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Built-in job for rewriting Iceberg table manifest files.
 *
 * <p>This job leverages Iceberg's RewriteManifestsProcedure to consolidate small manifest files
 * and rewrite manifests with improved partition specs, which improves scan planning performance.
 */
public class IcebergRewriteManifestsJob implements BuiltInJob {

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-rewrite-manifests";
  private static final String VERSION = "v1";

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "Built-in Iceberg rewrite manifests job template for scan planning optimization")
        .withExecutable(resolveExecutable(IcebergRewriteManifestsJob.class))
        .withClassName(IcebergRewriteManifestsJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Main entry point for the rewrite manifests job.
   *
   * <p>Uses named arguments for flexibility:
   *
   * <ul>
   *   <li>--catalog &lt;catalog_name&gt; Required. Iceberg catalog name.
   *   <li>--table &lt;table_identifier&gt; Required. Table name (db.table)
   *   <li>--use-caching &lt;boolean&gt; Optional. Whether to use caching (default: true)
   *   <li>--spark-conf &lt;spark_conf_json&gt; Optional. JSON map of custom Spark configurations
   * </ul>
   *
   * <p>Example via Gravitino API:
   *
   * <pre>{@code
   * Map<String, String> jobConf = new HashMap<>();
   * jobConf.put("catalog_name", "iceberg_catalog");
   * jobConf.put("table_identifier", "db.sample");
   * jobConf.put("use_caching", "true");
   * metalake.runJob("builtin-iceberg-rewrite-manifests", jobConf);
   * }</pre>
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      printUsage();
      System.exit(1);
    }

    Map<String, String> argMap = parseArguments(args);

    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");

    if (catalogName == null || tableIdentifier == null) {
      System.err.println("Error: --catalog and --table are required arguments");
      printUsage();
      System.exit(1);
    }

    String useCaching = argMap.get("use-caching");
    String sparkConfJson = argMap.get("spark-conf");

    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Rewrite Manifests");

    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      try {
        Map<String, String> customConfigs =
            IcebergRewriteDataFilesJob.parseCustomSparkConfigs(sparkConfJson);
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
      String sql = buildProcedureCall(catalogName, tableIdentifier, useCaching);
      System.out.println("Executing Iceberg rewrite_manifests procedure: " + sql);

      Row[] results = (Row[]) spark.sql(sql).collect();

      if (results.length > 0) {
        Row result = results[0];
        System.out.printf(
            "Rewrite Manifests Results:%n"
                + "  Rewritten manifests: %d%n"
                + "  Added manifests: %d%n",
            result.getInt(0), result.getInt(1));
      }

      System.out.println("Rewrite manifests job completed successfully");
    } catch (Exception e) {
      System.err.println("Error executing rewrite manifests job: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      spark.stop();
    }
  }

  /**
   * Build the SQL CALL statement for the rewrite_manifests procedure.
   *
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier Fully qualified table name
   * @param useCaching Whether to use caching during rewrite
   * @return SQL CALL statement
   */
  static String buildProcedureCall(
      String catalogName, String tableIdentifier, String useCaching) {
    StringBuilder sql = new StringBuilder();
    sql.append("CALL ")
        .append(IcebergRewriteDataFilesJob.escapeSqlIdentifier(catalogName))
        .append(".system.rewrite_manifests(");
    sql.append("table => '")
        .append(IcebergRewriteDataFilesJob.escapeSqlString(tableIdentifier))
        .append("'");

    if (useCaching != null && !useCaching.isEmpty()) {
      sql.append(", use_caching => ").append(Boolean.parseBoolean(useCaching));
    }

    sql.append(")");
    return sql.toString();
  }

  /**
   * Parse command line arguments in --key value format.
   *
   * @param args command line arguments
   * @return map of argument names to values
   */
  static Map<String, String> parseArguments(String[] args) {
    Map<String, String> argMap = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("--")) {
        String key = args[i].substring(2);
        if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
          String value = args[i + 1];
          if (value != null && !value.trim().isEmpty()) {
            argMap.put(key, value);
          }
          i++;
        } else {
          System.err.println("Warning: Flag " + args[i] + " has no value, ignoring");
        }
      }
    }
    return argMap;
  }

  private static void printUsage() {
    System.err.println(
        "Usage: IcebergRewriteManifestsJob [OPTIONS]\n"
            + "\n"
            + "Required Options:\n"
            + "  --catalog <name>          Iceberg catalog name registered in Spark\n"
            + "  --table <identifier>      Fully qualified table name (e.g., db.table_name)\n"
            + "\n"
            + "Optional Options:\n"
            + "  --use-caching <boolean>   Whether to use caching during rewrite (default: true)\n"
            + "  --spark-conf <json>       JSON map of custom Spark configurations\n"
            + "                              Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'\n"
            + "\n"
            + "Examples:\n"
            + "  # Basic rewrite\n"
            + "  --catalog iceberg_prod --table db.sample\n"
            + "\n"
            + "  # Without caching\n"
            + "  --catalog iceberg_prod --table db.sample --use-caching false\n");
  }

  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--use-caching",
        "{{use_caching}}",
        "--spark-conf",
        "{{spark_conf}}");
  }

  private static Map<String, String> buildSparkConfigs() {
    return IcebergSparkConfigUtils.buildTemplateSparkConfigs();
  }
}