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
 * Built-in job for rewriting Iceberg table manifest files.
 *
 * <p>This job leverages Iceberg's RewriteManifestsProcedure to consolidate small manifest files and
 * rewrite manifests with improved partition specs, which improves scan planning performance.
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
   *   <li>--use-caching &lt;boolean&gt; Optional. Whether to cache the table metadata in Spark
   *       while rewriting (default: Iceberg's own default)
   *   <li>--spec-id &lt;int&gt; Optional. Rewrite manifests to this partition spec ID (default: the
   *       table's current spec)
   *   <li>--spark-conf &lt;spark_conf_json&gt; Optional. JSON map of custom Spark configurations
   * </ul>
   *
   * <p><b>Important Notes on Special Characters:</b>
   *
   * <ul>
   *   <li><b>Via Gravitino API:</b> Pass values as-is without shell escaping. Gravitino handles
   *       escaping internally via ProcessBuilder.
   *   <li><b>Via Command Line:</b> Use shell quoting for values containing whitespace.
   * </ul>
   *
   * <p>Example via command line: --catalog iceberg_catalog --table db.sample --use-caching false
   *
   * <p>Example via Gravitino API:
   *
   * <pre>{@code
   * Map<String, String> jobConf = new HashMap<>();
   * jobConf.put("catalog_name", "iceberg_catalog");
   * jobConf.put("table_identifier", "db.sample");
   * jobConf.put("use_caching", "false");
   * metalake.runJob("builtin-iceberg-rewrite-manifests", jobConf);
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

    // Optional arguments. Unresolved template placeholders mean the caller left the parameter out,
    // so they are dropped rather than forwarded to Iceberg as literal values.
    String useCaching = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("use-caching"));
    String specId = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spec-id"));
    String sparkConfJson = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spark-conf"));

    // Validate optional arguments if provided
    try {
      validateUseCaching(useCaching);
      validateSpecId(specId);
    } catch (IllegalArgumentException e) {
      System.err.println("Error: " + e.getMessage());
      printUsage();
      System.exit(1);
    }

    // Build Spark session with custom configs if provided
    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Built-in Iceberg Rewrite Manifests");

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
      String sql = buildProcedureCall(catalogName, tableIdentifier, useCaching, specId);

      System.out.println("Executing Iceberg rewrite_manifests procedure: " + sql);

      // Execute the procedure
      List<Row> results = spark.sql(sql).collectAsList();

      // Print results. The procedure output columns are numeric, but their exact width is an
      // Iceberg implementation detail, so read them as Number rather than a fixed primitive.
      if (!results.isEmpty()) {
        Row result = results.get(0);
        System.out.printf(
            "Rewrite Manifests Results:%n"
                + "  Rewritten manifests: %d%n"
                + "  Added manifests: %d%n",
            ((Number) result.get(0)).longValue(), ((Number) result.get(1)).longValue());
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
   * @param useCaching Whether to cache table metadata during the rewrite
   * @param specId Partition spec ID to rewrite manifests to
   * @return SQL CALL statement
   */
  static String buildProcedureCall(
      String catalogName, String tableIdentifier, String useCaching, String specId) {
    StringBuilder sql = new StringBuilder();
    sql.append("CALL ")
        .append(IcebergJobUtils.escapeSqlIdentifier(catalogName))
        .append(".system.rewrite_manifests(");
    sql.append("table => '").append(IcebergJobUtils.escapeSqlString(tableIdentifier)).append("'");

    if (useCaching != null && !useCaching.isEmpty()) {
      sql.append(", use_caching => ").append(Boolean.parseBoolean(useCaching));
    }

    if (specId != null && !specId.isEmpty()) {
      sql.append(", spec_id => ").append(Integer.parseInt(specId));
    }

    sql.append(")");
    return sql.toString();
  }

  /**
   * Validate the use-caching parameter value.
   *
   * <p>{@link Boolean#parseBoolean(String)} maps anything that is not {@code "true"} to {@code
   * false}, so a typo would silently disable caching. Reject such values instead.
   *
   * @param useCaching the use-caching value to validate
   * @throws IllegalArgumentException if the value is neither "true" nor "false"
   */
  static void validateUseCaching(String useCaching) {
    if (useCaching == null || useCaching.isEmpty()) {
      return; // use-caching is optional
    }

    if (!"true".equalsIgnoreCase(useCaching) && !"false".equalsIgnoreCase(useCaching)) {
      throw new IllegalArgumentException(
          "Invalid use-caching value '" + useCaching + "'. Must be either 'true' or 'false'");
    }
  }

  /**
   * Validate the spec-id parameter value.
   *
   * <p>Iceberg partition spec IDs start at 0 and the procedure rejects an unknown ID, but failing
   * here keeps a malformed value from reaching Spark as an unparseable SQL literal.
   *
   * @param specId the spec-id value to validate
   * @throws IllegalArgumentException if the value is not a non-negative integer
   */
  static void validateSpecId(String specId) {
    if (specId == null || specId.isEmpty()) {
      return; // spec-id is optional
    }

    try {
      if (Integer.parseInt(specId) < 0) {
        throw new IllegalArgumentException(
            "Invalid spec-id value '" + specId + "'. Must be a non-negative integer");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid spec-id value '" + specId + "'. Must be a non-negative integer");
    }
  }

  /** Print usage information. */
  private static void printUsage() {
    System.err.println(
        "Usage: IcebergRewriteManifestsJob [OPTIONS]\n"
            + "\n"
            + "Required Options:\n"
            + "  --catalog <name>          Iceberg catalog name registered in Spark\n"
            + "  --table <identifier>      Fully qualified table name (e.g., db.table_name)\n"
            + "\n"
            + "Optional Options:\n"
            + "  --use-caching <boolean>   Cache table metadata in Spark while rewriting\n"
            + "                              Must be either 'true' or 'false'\n"
            + "                              Default: true (Iceberg default)\n"
            + "  --spec-id <int>           Rewrite manifests to this partition spec ID\n"
            + "                              Must be a non-negative integer\n"
            + "                              Default: the table's current partition spec\n"
            + "  --spark-conf <json>       JSON map of custom Spark configurations\n"
            + "                              Example: '{\"spark.sql.shuffle.partitions\":\"200\"}'\n"
            + "                              Note: Overriding required catalog/extensions/app-name configs is unsupported\n"
            + "\n"
            + "Examples:\n"
            + "  # Basic rewrite with Iceberg defaults\n"
            + "  --catalog iceberg_prod --table db.sample\n"
            + "\n"
            + "  # Rewrite without caching table metadata\n"
            + "  --catalog iceberg_prod --table db.sample --use-caching false\n"
            + "\n"
            + "  # Re-cluster manifests onto partition spec 2 after a spec evolution\n"
            + "  --catalog iceberg_prod --table db.sample --spec-id 2");
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
        "--use-caching",
        "{{use_caching}}",
        "--spec-id",
        "{{spec_id}}",
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
