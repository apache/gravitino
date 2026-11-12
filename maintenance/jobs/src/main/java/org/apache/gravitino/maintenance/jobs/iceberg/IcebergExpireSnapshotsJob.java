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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.iceberg.Snapshot;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in job for expiring old snapshots from Iceberg tables.
 *
 * <p>This job leverages Iceberg's ExpireSnapshots procedure to remove snapshot metadata and
 * associated data files that are no longer needed, preventing unbounded metadata growth.
 */
public class IcebergExpireSnapshotsJob implements BuiltInJob, IcebergMaintenanceAction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergExpireSnapshotsJob.class);

  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-expire-snapshots";
  private static final String VERSION = "v1";

  /** Full table inventory captured before expire; used to compute S1 set-diff. */
  @Nullable private PreExpireData preExpireData;

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in Iceberg expire snapshots job template for metadata cleanup")
        .withExecutable(resolveExecutable(IcebergExpireSnapshotsJob.class))
        .withClassName(IcebergExpireSnapshotsJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
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
   *   <li>--stream-results [true|false] Optional. Whether to stream intermediate results. A bare
   *       --stream-results means true
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
    String sparkConfJson = argMap.get("spark-conf");

    boolean streamResults = false;
    try {
      validateRetainLast(retainLast);
      streamResults = parseStreamResults(argMap.get("stream-results"));
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
      } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        printUsage();
        System.exit(1);
      }
    }

    SparkSession spark = sparkBuilder.getOrCreate();
    IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);

    try {
      List<Row> results =
          execute(spark, catalogName, tableIdentifier, olderThan, retainLast, streamResults);
      if (!results.isEmpty()) {
        Row result = results.get(0);
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
   * Runs {@code expire_snapshots} on an existing Spark session. Shared by CLI {@link #main} and TMS
   * {@link #executeAndCollectAfter}.
   *
   * @return procedure result rows
   */
  static List<Row> execute(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String olderThan,
      @Nullable String retainLast,
      boolean streamResults) {
    validateRetainLast(retainLast);
    String sql =
        buildProcedureCall(catalogName, tableIdentifier, olderThan, retainLast, streamResults);
    LOG.info("Executing Iceberg expire_snapshots: {}", sql);
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
    preExpireData = collectTableData(spark, catalogName, tableIdentifier);
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(
        IcebergSnapshotExpirationContent.SNAPSHOT_NUMBER_METRIC, preExpireData.snapshotCount());
    return TableSnapshotMetrics.of(snapshotId, metrics);
  }

  @Override
  public boolean shouldCheckSnapshotDrift() {
    return false;
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
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("older-than")),
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("retain-last")),
            parseStreamResults(
                IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("stream-results"))));
    if (!results.isEmpty()) {
      LOG.info(
          "Expire snapshots deleted data/manifest/manifest-list files: {}/{}/{}",
          results.get(0).getLong(0),
          results.get(0).getLong(1),
          results.get(0).getLong(2));
    }
    PreExpireData after = collectTableData(spark, catalogName, tableIdentifier);
    PreExpireData beforeData = preExpireData == null ? after : preExpireData;
    preExpireData = null;
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(
        IcebergSnapshotExpirationContent.EXPIRED_SNAPSHOT_NUMBER_METRIC,
        snapshotsCleared(beforeData.snapshotIds(), after.snapshotIds()));
    metrics.put(
        IcebergSnapshotExpirationContent.EXPIRED_FILE_SIZE_METRIC,
        bytesReclaimed(beforeData.fileSizesByPath(), after.fileSizesByPath()));
    return TableSnapshotMetrics.of(
        IcebergJobUtils.currentSnapshotIdOrEmpty(spark, catalogName, tableIdentifier), metrics);
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
   * Parse the value of {@code --stream-results}.
   *
   * @param value the raw value, which is {@code true} for a bare {@code --stream-results}, and null
   *     or blank when the option is absent
   * @return whether to stream intermediate results
   * @throws IllegalArgumentException if the value is neither true nor false
   */
  static boolean parseStreamResults(String value) {
    if (value == null || value.trim().isEmpty()) {
      return false;
    }
    String trimmed = value.trim();
    if ("true".equalsIgnoreCase(trimmed)) {
      return true;
    }
    if ("false".equalsIgnoreCase(trimmed)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid stream-results value '" + value + "'. Must be true or false");
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
    IcebergJobUtils.printCliUsage(
        IcebergExpireSnapshotsJob.class.getSimpleName(),
        ImmutableList.of(
            "  --catalog <name>          Iceberg catalog name registered in Spark",
            "  --table <identifier>      Fully qualified table name (e.g., db.table_name)"),
        IcebergJobUtils.withSparkConfUsage(
            "  --older-than <timestamp>  Expire snapshots older than this timestamp",
            "                              Example: '2024-01-01 00:00:00'",
            "                              Default: 5 days ago (Iceberg default)",
            "  --retain-last <count>     Number of most recent snapshots to retain",
            "                              Must be a positive integer (>= 1)",
            "                              Default: 1 (Iceberg default)",
            "  --stream-results [bool]   Stream intermediate delete results (true or false)",
            "                              A bare --stream-results means true"),
        ImmutableList.of(
            "  # Basic expire with defaults (5 days, retain 1)",
            "  --catalog iceberg_prod --table db.sample",
            "",
            "  # Expire snapshots older than a specific date",
            "  --catalog iceberg_prod --table db.sample --older-than '2024-01-01 00:00:00'",
            "",
            "  # Retain the last 5 snapshots",
            "  --catalog iceberg_prod --table db.sample --retain-last 5",
            "",
            "  # Expire with all options and streaming",
            "  --catalog iceberg_prod --table db.sample --older-than '2024-06-01 00:00:00' \\",
            "    --retain-last 3 --stream-results"));
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
        "{{older_than:-}}",
        "--retain-last",
        "{{retain_last:-}}",
        "--stream-results",
        "{{stream_results:-false}}",
        "--spark-conf",
        "{{spark_conf:-}}");
  }

  // -------------------------------------------------------------------------
  // TMS metrics helpers (not used by CLI {@link #main})
  // -------------------------------------------------------------------------

  /**
   * Full table inventory before expire (all snapshot ids and reachable files). Used with a
   * post-expire capture to compute S1 set-diff. Concurrent commits only add ids/paths to the after
   * set; cleared/expired count ids/paths that disappeared. File sizes stay in memory for S1 and are
   * not reported in S0 metrics.
   */
  @VisibleForTesting
  static final class PreExpireData {
    private final Set<Long> snapshotIds;
    private final Map<String, Long> fileSizesByPath;

    PreExpireData(Set<Long> snapshotIds, Map<String, Long> fileSizesByPath) {
      this.snapshotIds = Collections.unmodifiableSet(new HashSet<>(snapshotIds));
      this.fileSizesByPath = Collections.unmodifiableMap(new HashMap<>(fileSizesByPath));
    }

    Set<Long> snapshotIds() {
      return snapshotIds;
    }

    Map<String, Long> fileSizesByPath() {
      return fileSizesByPath;
    }

    long snapshotCount() {
      return snapshotIds.size();
    }
  }

  @VisibleForTesting
  static long snapshotsCleared(Set<Long> beforeSnapshotIds, Set<Long> afterSnapshotIds) {
    long cleared = 0L;
    for (Long snapshotId : beforeSnapshotIds) {
      if (!afterSnapshotIds.contains(snapshotId)) {
        cleared++;
      }
    }
    return cleared;
  }

  @VisibleForTesting
  static long bytesReclaimed(Map<String, Long> beforeFileSizes, Map<String, Long> afterFileSizes) {
    long bytes = 0L;
    for (Map.Entry<String, Long> entry : beforeFileSizes.entrySet()) {
      if (!afterFileSizes.containsKey(entry.getKey())) {
        bytes += entry.getValue() == null ? 0L : entry.getValue();
      }
    }
    return bytes;
  }

  /**
   * Collects full table data (snapshot ids + reachable files) for TMS set-diff. CLI entry points
   * never call this.
   *
   * <p>Uses {@code all_files} metadata SQL (same style as compaction's {@code files} SQL) plus
   * Iceberg snapshot ids.
   */
  @VisibleForTesting
  static PreExpireData collectTableData(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Set<Long> snapshotIds = new HashSet<>();
    for (Snapshot snapshot :
        IcebergJobUtils.loadIcebergTable(spark, catalogName, tableIdentifier).snapshots()) {
      snapshotIds.add(snapshot.snapshotId());
    }
    String sql = buildReachableContentFilesSql(catalogName, tableIdentifier);
    List<Row> rows = spark.sql(sql).collectAsList();
    Map<String, Long> fileSizesByPath = new HashMap<>(rows.size());
    for (Row row : rows) {
      String path = row.getAs("file-path");
      if (path == null || path.isEmpty()) {
        continue;
      }
      Number size = row.getAs("file-size");
      fileSizesByPath.put(path, size == null ? 0L : size.longValue());
    }
    return new PreExpireData(snapshotIds, fileSizesByPath);
  }

  /**
   * Distinct content files still referenced by any tracked snapshot. {@code GROUP BY file_path}
   * avoids double-counting files that appear in multiple snapshots.
   */
  @VisibleForTesting
  static String buildReachableContentFilesSql(String catalogName, String tableIdentifier) {
    String[] levels = tableIdentifier.split("\\.");
    if (levels.length != 2) {
      throw new IllegalArgumentException(
          "table_identifier must use schema.table format, but got: " + tableIdentifier);
    }
    String metadataTable =
        IcebergJobUtils.escapeSqlIdentifier(catalogName)
            + "."
            + IcebergJobUtils.escapeSqlIdentifier(levels[0])
            + "."
            + IcebergJobUtils.escapeSqlIdentifier(levels[1])
            + "."
            + IcebergJobUtils.escapeSqlIdentifier("all_files");
    return "SELECT file_path AS `file-path`, MAX(file_size_in_bytes) AS `file-size` FROM "
        + metadataTable
        + " GROUP BY file_path";
  }
}
