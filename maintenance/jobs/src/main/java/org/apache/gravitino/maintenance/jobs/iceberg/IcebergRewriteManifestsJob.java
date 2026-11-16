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
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in job for rewriting Iceberg table manifest files.
 *
 * <p>This job leverages Iceberg's RewriteManifestsProcedure to consolidate small manifest files and
 * cluster manifest entries within an existing partition spec to improve scan planning.
 */
public class IcebergRewriteManifestsJob implements BuiltInJob, IcebergMaintenanceAction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteManifestsJob.class);

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
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
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
   *   <li>--spec-id &lt;int&gt; Optional. Rewrite manifests belonging to this partition spec ID
   *       (default: the table's current spec)
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
   *
   * @param args named job arguments
   */
  public static void main(String[] args) {
    int exitCode = run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args) {
    try {
      Map<String, String> argMap = parseArguments(args);
      Map<String, String> configs =
          IcebergJobUtils.parseCustomSparkConfigs(argMap.get("spark-conf"));
      SparkSession.Builder builder =
          SparkSession.builder().appName("Gravitino Built-in Iceberg Rewrite Manifests");
      configs.forEach(builder::config);

      try (SparkSession spark = builder.getOrCreate()) {
        IcebergJobUtils.requireIcebergSparkRuntime();
        List<Row> results =
            execute(
                spark,
                argMap.get("catalog"),
                argMap.get("table"),
                argMap.get("use-caching"),
                argMap.get("spec-id"));
        if (!results.isEmpty()) {
          Row result = results.get(0);
          System.out.printf(
              "Rewrite Manifests Results: Rewritten manifests: %d, Added manifests: %d%n",
              ((Number) result.get(0)).longValue(), ((Number) result.get(1)).longValue());
        }
        System.out.println("Rewrite manifests job completed successfully");
      }
      return 0;
    } catch (Exception e) {
      System.err.println("Error rewriting manifests: " + e.getMessage());
      printUsage();
      return 1;
    }
  }

  static Map<String, String> parseArguments(String[] args) {
    Map<String, String> parsed =
        IcebergJobUtils.parseArguments(
            args,
            new HashSet<>(
                Arrays.asList("catalog", "table", "use-caching", "spec-id", "spark-conf")),
            new HashSet<>(Arrays.asList("catalog", "table")));
    validateUseCaching(parsed.get("use-caching"));
    validateSpecId(parsed.get("spec-id"));
    return parsed;
  }

  /**
   * Build the SQL CALL statement for the rewrite_manifests procedure.
   *
   * @param catalogName Iceberg catalog name
   * @param tableIdentifier Fully qualified table name
   * @param useCaching Whether to cache table metadata during the rewrite
   * @param specId Existing partition spec ID whose manifests to rewrite
   * @return SQL CALL statement
   */
  static String buildProcedureCall(
      String catalogName,
      String tableIdentifier,
      @Nullable String useCaching,
      @Nullable String specId) {
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
  static void validateUseCaching(@Nullable String useCaching) {
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
  static void validateSpecId(@Nullable String specId) {
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

  /**
   * Runs {@code rewrite_manifests} on an existing Spark session. Shared by CLI and TMS {@link
   * IcebergMaintenanceJob}.
   *
   * @return procedure result rows
   */
  static List<Row> execute(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String useCaching,
      @Nullable String specId) {
    validateUseCaching(useCaching);
    validateSpecId(specId);
    String sql = buildProcedureCall(catalogName, tableIdentifier, useCaching, specId);
    LOG.info("Executing Iceberg rewrite_manifests: {}", sql);
    return spark.sql(sql).collectAsList();
  }

  @Override
  public boolean hasBeforeMetrics() {
    return true;
  }

  @Override
  public TableSnapshotMetrics collectBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    long snapshotId = IcebergJobUtils.requireCurrentSnapshotId(spark, catalogName, tableIdentifier);
    Map<String, Object> metrics = collectManifestBeforeMetrics(spark, catalogName, tableIdentifier);
    return TableSnapshotMetrics.of(snapshotId, metrics);
  }

  @Override
  public boolean shouldCheckSnapshotDrift() {
    return true;
  }

  @Override
  public TableSnapshotMetrics executeAndCollectAfter(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argsByName,
      @Nullable TableSnapshotMetrics before) {
    List<Row> results =
        execute(
            spark,
            catalogName,
            tableIdentifier,
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("use-caching")),
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spec-id")));
    long rewritten = 0L;
    if (!results.isEmpty()) {
      Row result = results.get(0);
      rewritten = ((Number) result.get(0)).longValue();
      LOG.info(
          "Rewrite manifests results: rewritten={}, added={}",
          rewritten,
          ((Number) result.get(1)).longValue());
    }
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergRewriteManifestsContent.REWRITTEN_MANIFEST_NUMBER_METRIC, rewritten);
    return TableSnapshotMetrics.of(
        IcebergJobUtils.requireCurrentSnapshotId(spark, catalogName, tableIdentifier), metrics);
  }

  private static void printUsage() {
    IcebergJobUtils.printCliUsage(
        IcebergRewriteManifestsJob.class.getSimpleName(),
        ImmutableList.of(
            "  --catalog <name>          Iceberg catalog name registered in Spark",
            "  --table <identifier>      Fully qualified table name (e.g., db.table_name)"),
        IcebergJobUtils.withSparkConfUsage(
            "  --use-caching <bool>      Whether to use caching during rewrite",
            "  --spec-id <int>           Non-negative partition spec id to rewrite"),
        ImmutableList.of(
            "  --catalog iceberg_prod --table db.sample",
            "  --catalog iceberg_prod --table db.sample --use-caching false --spec-id 0"));
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
        "{{use_caching:-}}",
        "--spec-id",
        "{{spec_id:-}}",
        "--spark-conf",
        "{{spark_conf:-}}");
  }

  // -------------------------------------------------------------------------
  // TMS metrics helpers (not used by CLI {@link #main})
  // -------------------------------------------------------------------------

  /**
   * Collects manifest inventory for TMS S0. Reads the current snapshot's manifests via Iceberg API.
   * CLI entry points never call this.
   */
  @VisibleForTesting
  static Map<String, Object> collectManifestBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Table table = IcebergJobUtils.loadIcebergTable(spark, catalogName, tableIdentifier);
    Snapshot snapshot = table.currentSnapshot();
    long count = 0L;
    long totalSize = 0L;
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        count++;
        totalSize += manifest.length();
      }
    }
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC, count);
    metrics.put(
        IcebergRewriteManifestsContent.AVG_MANIFEST_SIZE_METRIC,
        count == 0L ? 0D : (double) totalSize / count);
    return metrics;
  }
}
