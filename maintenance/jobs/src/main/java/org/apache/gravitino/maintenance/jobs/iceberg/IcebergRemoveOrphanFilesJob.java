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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in job for removing unreferenced Iceberg files after validating the requested scan
 * location.
 */
public class IcebergRemoveOrphanFilesJob implements BuiltInJob, IcebergMaintenanceAction {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergRemoveOrphanFilesJob.class);
  private static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-remove-orphan-files";
  private static final String VERSION = "v1";

  /**
   * Outcome of orphan cleanup with byte measurement.
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
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Runs orphan file cleanup using named arguments.
   *
   * <p>Required: {@code --catalog name --table db.table}. Optional: {@code --older-than 'yyyy-MM-dd
   * HH:mm:ss'}, {@code --location path}, {@code --dry-run true|false}, and {@code --spark-conf
   * json}.
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
    try {
      Map<String, String> argMap =
          IcebergJobUtils.parseArguments(
              args,
              new HashSet<>(
                  Arrays.asList(
                      "catalog", "table", "older-than", "location", "dry-run", "spark-conf")),
              new HashSet<>(Arrays.asList("catalog", "table")));
      boolean dryRun = parseDryRun(argMap.get("dry-run"));
      Map<String, String> configs =
          IcebergJobUtils.parseCustomSparkConfigs(argMap.get("spark-conf"));
      SparkSession.Builder builder =
          SparkSession.builder().appName("Gravitino Built-in Iceberg Remove Orphan Files");
      configs.forEach(builder::config);

      try (SparkSession spark = builder.getOrCreate()) {
        IcebergJobUtils.requireIcebergSparkRuntime();
        List<Row> results =
            execute(
                spark,
                argMap.get("catalog"),
                argMap.get("table"),
                IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("older-than")),
                IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("location")),
                dryRun);
        LOG.info(
            "Orphan file cleanup completed: {} files {}, table {}.{}",
            results.size(),
            dryRun ? "found (dry-run)" : "removed",
            argMap.get("catalog"),
            argMap.get("table"));
      }
      return 0;
    } catch (Exception e) {
      LOG.error("Error executing remove orphan files job", e);
      printUsage();
      return 1;
    }
  }

  /**
   * Runs {@code remove_orphan_files} on an existing Spark session. Shared by CLI {@link #main} and
   * tests. Does not measure reclaimed bytes.
   *
   * @return procedure result rows (one path per row)
   */
  static List<Row> execute(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String olderThan,
      @Nullable String location,
      boolean dryRun)
      throws IOException {
    requireEscapedStringLiteralsDisabled(spark);
    String normalizedLocation =
        resolveAndValidateLocation(spark, catalogName, tableIdentifier, location);
    return callRemoveOrphanFiles(
        spark, catalogName, tableIdentifier, olderThan, normalizedLocation, dryRun);
  }

  /**
   * TMS path: dry-run list → FileIO sizes → optional real delete. CLI uses {@link #execute} instead
   * (single procedure call, no byte measurement).
   */
  static OrphanCleanupResult executeWithMetrics(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String olderThan,
      @Nullable String location,
      boolean dryRun)
      throws IOException {
    requireEscapedStringLiteralsDisabled(spark);
    Table table = IcebergJobUtils.loadIcebergTable(spark, catalogName, tableIdentifier);
    String normalizedLocation = resolveAndValidateLocation(spark, table, location);
    List<Row> listed =
        callRemoveOrphanFiles(
            spark, catalogName, tableIdentifier, olderThan, normalizedLocation, true);
    List<String> orphanPaths = new ArrayList<>(listed.size());
    long bytes = 0L;
    for (Row row : listed) {
      String orphanPath = row.getString(0);
      orphanPaths.add(orphanPath);
      if (dryRun) {
        LOG.info("Orphan file (dry-run): {}", orphanPath);
      }
      bytes += sizeOf(table, orphanPath);
    }
    if (!dryRun && !orphanPaths.isEmpty()) {
      callRemoveOrphanFiles(
          spark, catalogName, tableIdentifier, olderThan, normalizedLocation, false);
    }
    LOG.info(
        "Orphan file cleanup completed: {} files {}, {} bytes, table {}.{}",
        orphanPaths.size(),
        dryRun ? "found (dry-run)" : "removed",
        bytes,
        catalogName,
        tableIdentifier);
    return new OrphanCleanupResult(orphanPaths.size(), bytes);
  }

  private static List<Row> callRemoveOrphanFiles(
      SparkSession spark,
      String catalogName,
      String tableIdentifier,
      @Nullable String olderThan,
      String normalizedLocation,
      boolean dryRun) {
    String sql =
        buildProcedureCall(catalogName, tableIdentifier, olderThan, normalizedLocation, dryRun);
    LOG.info("Executing Iceberg remove_orphan_files: {}", sql);
    return spark.sql(sql).collectAsList();
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
        executeWithMetrics(
            spark,
            catalogName,
            tableIdentifier,
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("older-than")),
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("location")),
            parseDryRun(IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("dry-run"))));
    Map<String, Object> payload = new HashMap<>();
    payload.put(IcebergOrphanFileRemovalContent.REMOVED_FILE_NUMBER_METRIC, orphan.filesRemoved());
    payload.put(IcebergOrphanFileRemovalContent.REMOVED_FILE_SIZE_METRIC, orphan.bytesReclaimed());
    return TableSnapshotMetrics.of(
        IcebergJobUtils.currentSnapshotIdOrEmpty(spark, catalogName, tableIdentifier), payload);
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

  private static void requireEscapedStringLiteralsDisabled(SparkSession spark) {
    // Backslash escapes in string literals must retain Spark's default interpretation.
    Preconditions.checkArgument(
        !Boolean.parseBoolean(spark.conf().get("spark.sql.parser.escapedStringLiterals", "false")),
        "spark.sql.parser.escapedStringLiterals must be false");
  }

  private static String resolveAndValidateLocation(
      SparkSession spark, String catalogName, String tableIdentifier, @Nullable String location)
      throws IOException {
    Table table = IcebergJobUtils.loadIcebergTable(spark, catalogName, tableIdentifier);
    return resolveAndValidateLocation(spark, table, location);
  }

  private static String resolveAndValidateLocation(
      SparkSession spark, Table table, @Nullable String location) throws IOException {
    // Direct callers may pass an empty template value without CLI argument parsing.
    if (location == null || location.codePoints().allMatch(Character::isWhitespace)) {
      location = table.location();
    }
    OrphanFileLocationUtils.validateLocation(table.location(), location);
    validateLocalLocation(table.location(), location);
    validateRemoteLocation(spark, table.location(), location);
    return normalizeLocation(location).toString();
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

  private static void printUsage() {
    IcebergJobUtils.printCliUsage(
        IcebergRemoveOrphanFilesJob.class.getSimpleName(),
        ImmutableList.of(
            "  --catalog <name>          Iceberg catalog name registered in Spark",
            "  --table <identifier>      Fully qualified table name (e.g., db.table_name)"),
        IcebergJobUtils.withSparkConfUsage(
            "  --older-than <timestamp>  Remove orphans older than this timestamp",
            "                              Example: '2024-01-01 00:00:00'",
            "  --location <path>         Orphan scan root location",
            "  --dry-run <bool>          List orphans without deleting (true or false)"),
        ImmutableList.of(
            "  --catalog iceberg_prod --table db.sample --dry-run true",
            "  --catalog iceberg_prod --table db.sample --older-than '2024-01-01 00:00:00'"));
  }
}
