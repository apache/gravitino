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

import static org.apache.gravitino.maintenance.optimizer.common.util.OrphanFileLocationUtils.normalizeLocation;
import static org.apache.spark.sql.functions.lit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.OrphanFileLocationUtils;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Removes unreferenced Iceberg files after validating the requested scan location. */
public class IcebergRemoveOrphanFilesJob implements BuiltInJob, IcebergMaintenanceAction {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRemoveOrphanFilesJob.class);
  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-remove-orphan-files";
  private static final String VERSION = "v1";

  /**
   * Outcome of a TMS orphan cleanup with byte measurement.
   *
   * <p>Only produced by {@link #executeWithMetrics}; CLI {@link #execute} does not collect bytes.
   */
  public static final class OrphanCleanupResult {
    private final long filesRemoved;
    private final long bytesReclaimed;

    OrphanCleanupResult(long filesRemoved, long bytesReclaimed) {
      this.filesRemoved = filesRemoved;
      this.bytesReclaimed = bytesReclaimed;
    }

    /**
     * @return orphan files found (dry-run) or removed
     */
    public long filesRemoved() {
      return filesRemoved;
    }

    /**
     * @return sum of file sizes measured before delete; {@code 0} when every size lookup failed
     */
    public long bytesReclaimed() {
      return bytesReclaimed;
    }
  }

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment("Built-in Iceberg orphan file cleanup job template")
        .withExecutable(resolveExecutable(IcebergRemoveOrphanFilesJob.class))
        .withClassName(IcebergRemoveOrphanFilesJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(buildSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Runs orphan file cleanup using named arguments.
   *
   * <p>Required: {@code --catalog name --table db.table}. Optional: {@code --older-than 'yyyy-MM-dd
   * HH:mm:ss'}, {@code --location path}, {@code --dry-run true|false}, and {@code --spark-conf
   * json}. The cutoff defaults to three days ago and dry-run defaults to false. Iceberg's minimum
   * retention interval is preserved. A custom location must be within the table.
   *
   * @param args named command-line arguments
   */
  public static void main(String[] args) {
    int exitCode = run(args);
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  static int run(String[] args) {
    return Runner.run(args);
  }

  /**
   * CLI / standalone path: run {@code remove_orphan_files} once and return the row count. Does
   * <b>not</b> measure bytes or double-list for metrics.
   */
  static long execute(SparkSession spark, Map<String, String> options)
      throws IOException, AnalysisException {
    PreparedOrphanCall prepared = prepare(spark, options);
    boolean dryRun = parseDryRun(options.get("dry-run"));
    String sql =
        buildProcedureCall(
            prepared.catalog,
            prepared.identifier,
            options.get("older-than"),
            prepared.normalizedLocation,
            dryRun);
    java.util.Iterator<Row> results = spark.sql(sql).toLocalIterator();
    long count = 0;
    while (results.hasNext()) {
      Row row = results.next();
      if (dryRun) {
        LOG.info("Orphan file (dry-run): {}", row.getString(0));
      }
      count++;
    }
    LOG.info(
        "Orphan file cleanup completed: {} files {}, table {}.{}",
        count,
        dryRun ? "found (dry-run)" : "removed",
        prepared.catalog,
        prepared.identifier);
    return count;
  }

  /**
   * TMS path: builds options from job args then runs {@link #executeWithMetrics(SparkSession,
   * Map)}.
   */
  static OrphanCleanupResult executeWithMetrics(
      SparkSession spark, String catalogName, String tableIdentifier, Map<String, String> jobArgs)
      throws IOException, AnalysisException {
    Map<String, String> options = new HashMap<>();
    options.put("catalog", catalogName);
    options.put("table", tableIdentifier);
    putIfPresent(options, "older-than", jobArgs.get("older-than"));
    putIfPresent(options, "location", jobArgs.get("location"));
    putIfPresent(options, "dry-run", jobArgs.get("dry-run"));
    return executeWithMetrics(spark, options);
  }

  @Override
  public boolean hasBeforeMetrics() {
    return false;
  }

  @Override
  public TableSnapshotMetrics collectBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    throw new IllegalStateException("Orphan cleanup must not sample before-metrics");
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
      @Nullable TableSnapshotMetrics before)
      throws Exception {
    OrphanCleanupResult orphan =
        executeWithMetrics(spark, catalogName, tableIdentifier, argsByName);
    return TableSnapshotMetrics.of(
        IcebergMaintenanceSnapshots.currentIdOrEmpty(spark, catalogName, tableIdentifier),
        orphanAfterMetrics(orphan.filesRemoved(), orphan.bytesReclaimed()));
  }

  /**
   * TMS-only path: dry-run list → FileIO sizes → optional real delete. CLI {@link #main} uses
   * {@link #execute(SparkSession, Map)} instead (single procedure call, no byte measurement).
   */
  static OrphanCleanupResult executeWithMetrics(SparkSession spark, Map<String, String> options)
      throws IOException, AnalysisException {
    PreparedOrphanCall prepared = prepare(spark, options);
    boolean dryRun = parseDryRun(options.get("dry-run"));
    List<String> orphanPaths =
        listOrphanPaths(
            spark,
            prepared.catalog,
            prepared.identifier,
            options.get("older-than"),
            prepared.normalizedLocation);
    long bytes = 0L;
    for (String orphanPath : orphanPaths) {
      if (dryRun) {
        LOG.info("Orphan file (dry-run): {}", orphanPath);
      }
      bytes += sizeOf(prepared.table, orphanPath);
    }
    if (!dryRun && !orphanPaths.isEmpty()) {
      String deleteSql =
          buildProcedureCall(
              prepared.catalog,
              prepared.identifier,
              options.get("older-than"),
              prepared.normalizedLocation,
              false);
      LOG.info("Executing Iceberg remove_orphan_files delete: {}", deleteSql);
      spark.sql(deleteSql).collect();
    }
    LOG.info(
        "Orphan file cleanup completed: {} files {}, {} bytes, table {}.{}",
        orphanPaths.size(),
        dryRun ? "found (dry-run)" : "removed",
        bytes,
        prepared.catalog,
        prepared.identifier);
    return new OrphanCleanupResult(orphanPaths.size(), bytes);
  }

  @VisibleForTesting
  static Map<String, Object> orphanAfterMetrics(long removedFileNumber, long removedFileSize) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergOrphanFileRemovalContent.REMOVED_FILE_NUMBER_METRIC, removedFileNumber);
    metrics.put(IcebergOrphanFileRemovalContent.REMOVED_FILE_SIZE_METRIC, removedFileSize);
    return metrics;
  }

  private static PreparedOrphanCall prepare(SparkSession spark, Map<String, String> options)
      throws IOException, AnalysisException {
    String catalog = requireOption(options, "catalog");
    String identifier = requireOption(options, "table");
    // Backslash escapes in string literals must retain Spark's default interpretation.
    Preconditions.checkArgument(
        !Boolean.parseBoolean(spark.conf().get("spark.sql.parser.escapedStringLiterals", "false")),
        "spark.sql.parser.escapedStringLiterals must be false");
    Table table =
        Spark3Util.loadIcebergTable(
            spark, IcebergJobUtils.escapeSqlIdentifier(catalog) + "." + identifier);
    String location = options.get("location");
    // Direct callers may pass an empty template value without CLI argument parsing.
    if (location == null || location.codePoints().allMatch(Character::isWhitespace)) {
      location = table.location();
    }
    validateLocation(table.location(), location);
    validateLocalLocation(table.location(), location);
    validateRemoteLocation(spark, table.location(), location);
    return new PreparedOrphanCall(
        catalog, identifier, table, normalizeLocation(location).toString());
  }

  private static List<String> listOrphanPaths(
      SparkSession spark,
      String catalog,
      String identifier,
      @Nullable String olderThan,
      String location) {
    String sql = buildProcedureCall(catalog, identifier, olderThan, location, true);
    LOG.info("Executing Iceberg remove_orphan_files dry-run listing: {}", sql);
    List<Row> rows = spark.sql(sql).collectAsList();
    List<String> paths = new ArrayList<>(rows.size());
    for (Row row : rows) {
      paths.add(row.getString(0));
    }
    return paths;
  }

  /**
   * Best-effort size for one orphan path via the table {@link org.apache.iceberg.io.FileIO}.
   * Missing or unreadable files contribute {@code 0}.
   */
  @VisibleForTesting
  static long sizeOf(Table table, String location) {
    try {
      return table.io().newInputFile(location).getLength();
    } catch (RuntimeException e) {
      LOG.warn("Failed to measure orphan file size for {}: {}", location, e.toString());
      return 0L;
    }
  }

  private static final class PreparedOrphanCall {
    private final String catalog;
    private final String identifier;
    private final Table table;
    private final String normalizedLocation;

    private PreparedOrphanCall(
        String catalog, String identifier, Table table, String normalizedLocation) {
      this.catalog = catalog;
      this.identifier = identifier;
      this.table = table;
      this.normalizedLocation = normalizedLocation;
    }
  }

  static String buildProcedureCall(
      String catalog,
      String table,
      @Nullable String olderThan,
      @Nullable String location,
      boolean dryRun) {
    // Render string literals using Spark so quotes and backslashes round-trip correctly.
    StringBuilder sql =
        new StringBuilder("CALL ")
            .append(IcebergJobUtils.escapeSqlIdentifier(catalog))
            .append(".system.remove_orphan_files(table => ")
            .append(lit(table).expr().sql());
    if (olderThan != null && !olderThan.isEmpty()) {
      sql.append(", older_than => TIMESTAMP ").append(lit(olderThan).expr().sql());
    }
    if (location != null && !location.isEmpty()) {
      sql.append(", location => ").append(lit(location).expr().sql());
    }
    return sql.append(", dry_run => ").append(dryRun).append(")").toString();
  }

  static boolean parseDryRun(@Nullable String value) {
    Preconditions.checkArgument(
        value == null || "false".equals(value) || "true".equals(value),
        "--dry-run must be true or false");
    return "true".equals(value);
  }

  static void validateLocation(String tableLocation, String location) {
    OrphanFileLocationUtils.validateLocation(tableLocation, location);
  }

  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--older-than",
        "{{older_than:-}}",
        "--location",
        "{{location:-}}",
        "--dry-run",
        "{{dry_run:-false}}",
        "--spark-conf",
        "{{spark_conf:-}}");
  }

  private static Map<String, String> buildSparkConfigs() {
    return IcebergSparkConfigUtils.buildTemplateSparkConfigs();
  }

  private static void printUsage() {
    LOG.error(
        "Usage: IcebergRemoveOrphanFilesJob --catalog <name> --table <db.table> "
            + "[--older-than 'yyyy-MM-dd HH:mm:ss'] [--location <path>] "
            + "[--dry-run true|false] [--spark-conf <json>]");
  }

  private static void validateLocalLocation(String tableLocation, String location)
      throws IOException {
    URI requested = normalizeLocation(location);
    if (!"file".equals(requested.getScheme())) {
      return;
    }
    Path lexicalRoot = Paths.get(normalizeLocation(tableLocation));
    Path root = lexicalRoot.toRealPath();
    Path scan = Paths.get(requested);
    Preconditions.checkArgument(
        scan.toRealPath().startsWith(root),
        "Scan location resolves outside the table's storage location");
    for (Path ancestor = scan;
        ancestor != null && ancestor.startsWith(lexicalRoot);
        ancestor = ancestor.getParent()) {
      Preconditions.checkArgument(
          !Files.isSymbolicLink(ancestor), "Symlinks are not allowed in the scan location");
    }
    // Do not follow symlinks during validation. Iceberg must never list another table through one.
    try (Stream<Path> paths = Files.walk(scan)) {
      Preconditions.checkArgument(
          paths.noneMatch(Files::isSymbolicLink), "Symlinks are not allowed in the scan location");
    }
  }

  private static void validateRemoteLocation(
      SparkSession spark, String tableLocation, String location) throws IOException {
    if ("file".equals(normalizeLocation(location).getScheme())) {
      return;
    }
    RemoteLocationValidator.validate(
        spark.sparkContext().hadoopConfiguration(), tableLocation, location);
  }

  private static void putIfPresent(Map<String, String> target, String key, @Nullable String raw) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  private static String requireOption(Map<String, String> options, String key) {
    String value = options.get(key);
    Preconditions.checkArgument(value != null && !value.trim().isEmpty(), "--%s is required", key);
    return value;
  }

  // Defer verification of Spark-specific exception handlers until job execution. The server
  // loads this job's template without Spark or Iceberg on its classpath.
  private static final class Runner {
    private static int run(String[] args) {
      Map<String, String> options = IcebergJobUtils.parseArguments(args);
      SparkSession.Builder builder =
          SparkSession.builder().appName("Gravitino Built-in Iceberg Remove Orphan Files");
      try {
        requireOption(options, "catalog");
        requireOption(options, "table");
        parseDryRun(options.get("dry-run"));
        IcebergJobUtils.parseCustomSparkConfigs(options.get("spark-conf")).forEach(builder::config);
      } catch (IllegalArgumentException e) {
        LOG.error("Invalid remove orphan files job arguments: {}", e.getMessage());
        printUsage();
        return 1;
      }

      SparkSession spark = null;
      try {
        spark = builder.getOrCreate();
        IcebergJobUtils.requireIcebergSparkRuntime();
        execute(spark, options);
        return 0;
      } catch (IOException | AnalysisException | RuntimeException e) {
        LOG.error("Error executing remove orphan files job", e);
        return 1;
      } finally {
        if (spark != null) {
          spark.stop();
        }
      }
    }
  }
}
