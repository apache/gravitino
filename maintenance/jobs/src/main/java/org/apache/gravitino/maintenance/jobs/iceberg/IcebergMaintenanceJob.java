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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.jobs.MaintenanceActionHttpClient;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TMS Spark job: sample before-metrics → gate → run Iceberg procedure → after-metrics → close soft
 * slot.
 *
 * <p>Started only from the built-in template (cron or commit-chain). {@code maintenance_types} is
 * one type or a comma-separated chain; {@code policy_ids} / {@code action_ids} align 1:1 with those
 * types. Soft-slot HTTP callbacks use {@code action_ids}.
 *
 * <p>{@code metricsJson} carries only metric fields (no {@code maintenanceType} / {@code
 * snapshotId}; those live on the action/policy and {@code table_snapshot_metrics.snapshot_id}).
 * Snapshot drift skips only compaction and manifest rewrite. Orphan cleanup reports after-metrics
 * only.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /** Built-in template name used by TMS submit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  // Bump when template behavior / args / class wiring changes (required by built-in registration).
  private static final String VERSION = "v2";

  /** JobConf: one wire name, or comma-separated commit chain. */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /** JobConf: JSON array of {@code table_id}, {@code catalog_name}, {@code table_identifier}. */
  public static final String TABLES_KEY = "tables";

  /** JobConf: policy ids parallel to {@link #MAINTENANCE_TYPES_KEY}. */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /** JobConf: soft-slot action ids parallel to {@link #POLICY_IDS_KEY}. */
  public static final String ACTION_IDS_KEY = "action_ids";

  /** Compaction / inventory: average data file size in bytes. */
  public static final String AVG_FILE_SIZE_METRIC = "custom-avg-file-size";

  /** Compaction / inventory: total data+delete file size in bytes. */
  public static final String TOTAL_FILE_SIZE_METRIC = "custom-total-file-size";

  /** Compaction / inventory: total file count. */
  public static final String FILE_NUMBER_METRIC = "custom-file-number";

  /** Manifest rewrite S0: total manifest count across specs. */
  public static final String MANIFEST_NUMBER_METRIC =
      IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC;

  /** Manifest rewrite S0: average manifest size in bytes. */
  public static final String AVG_MANIFEST_SIZE_METRIC =
      IcebergRewriteManifestsContent.AVG_MANIFEST_SIZE_METRIC;

  /** Manifest rewrite S1: manifests rewritten/removed by the procedure. */
  public static final String MANIFESTS_REMOVED_METRIC = "custom-manifests-removed";

  /** Snapshot expiry S0: snapshot count before expire. */
  public static final String SNAPSHOT_COUNT_METRIC = "custom-snapshot-count";

  /** Snapshot expiry S1: snapshots removed by expire. */
  public static final String SNAPSHOTS_CLEARED_METRIC = "custom-snapshots-cleared";

  /** Snapshot expiry S1: bytes reclaimed (file-size drop after expire). */
  public static final String BYTES_RECLAIMED_METRIC = "custom-bytes-reclaimed";

  /** Orphan cleanup S1: orphan files removed (or found in dry-run). */
  public static final String ORPHAN_FILES_REMOVED_METRIC = "custom-orphan-files-removed";

  /**
   * Orphan cleanup S1: bytes reclaimed. Measured from orphan paths before delete (procedure returns
   * locations only).
   */
  public static final String ORPHAN_BYTES_RECLAIMED_METRIC = "custom-orphan-bytes-reclaimed";

  /** Values accepted in {@code maintenance_types}. */
  public enum MaintenanceType {
    REWRITE_DATA_FILES("rewrite_data_files"),
    REWRITE_MANIFESTS("rewrite_manifests"),
    EXPIRE_SNAPSHOTS("expire_snapshots"),
    REMOVE_ORPHAN_FILES("remove_orphan_files");

    private final String wireName;

    MaintenanceType(String wireName) {
      this.wireName = wireName;
    }

    /**
     * @return jobConf wire value
     */
    public String wireName() {
      return wireName;
    }

    /**
     * @param raw one segment from {@code maintenance_types}
     * @return parsed type
     */
    public static MaintenanceType fromWire(@Nullable String raw) {
      if (raw == null || raw.trim().isEmpty()) {
        throw new IllegalArgumentException("maintenance_types is required");
      }
      String normalized = raw.trim().toLowerCase(Locale.ROOT);
      for (MaintenanceType type : values()) {
        if (type.wireName.equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "Unknown maintenance_types value '"
              + raw
              + "'. Expected one of: rewrite_data_files, rewrite_manifests, expire_snapshots,"
              + " remove_orphan_files");
    }
  }

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "TMS Iceberg maintenance (before-metrics, gate, procedure, after-metrics, finish)")
        .withExecutable(resolveExecutable(IcebergMaintenanceJob.class))
        .withClassName(IcebergMaintenanceJob.class.getName())
        .withArguments(templateArguments())
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * Spark entry after template expansion.
   *
   * @param args named jobConf arguments
   */
  public static void main(String[] args) {
    int code = run(args);
    if (code != 0) {
      System.exit(code);
    }
  }

  /**
   * @param args named jobConf arguments
   * @return {@code 0} on success, {@code 1} on failure
   */
  static int run(String[] args) {
    Map<String, String> argsByName = IcebergJobUtils.parseArguments(args);

    List<MaintenanceType> types;
    List<Long> policyIds;
    List<Long> actionIds;
    List<TargetTable> tables;
    Map<String, String> updaterOptions;
    try {
      types = parseMaintenanceTypes(argsByName.get("maintenance-types"));
      policyIds = parseIdList("policy_ids", argsByName.get("policy-ids"), types.size());
      actionIds = parseIdList("action_ids", argsByName.get("action-ids"), types.size());
      tables = parseTables(IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("tables")));
      updaterOptions =
          parseUpdaterOptions(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("updater-options")));
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOG.error("Invalid TMS maintenance arguments: {}", e.getMessage());
      return 1;
    }

    String appLabel =
        types.size() == 1
            ? types.get(0).wireName
            : "chain:" + types.stream().map(t -> t.wireName).collect(Collectors.joining(","));
    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Iceberg Maintenance Task (" + appLabel + ")");
    String sparkConfJson =
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spark-conf"));
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson).forEach(sparkBuilder::config);
    }

    try (SparkSession spark = sparkBuilder.getOrCreate()) {
      IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);
      try (MaintenanceActionHttpClient client =
          MaintenanceActionHttpClient.fromUpdaterOptions(updaterOptions)) {
        // Close every soft slot even if a later type/table never runs.
        Set<String> closedSlots = new HashSet<>();
        try {
          for (TargetTable table : tables) {
            for (int i = 0; i < types.size(); i++) {
              MaintenanceType type = types.get(i);
              long policyId = policyIds.get(i);
              long actionId = actionIds.get(i);
              String slot = table.tableId + ":" + policyId;
              try {
                maintainOneType(spark, client, type, table, policyId, actionId, argsByName);
              } finally {
                closeSoftSlot(client, actionId, table.tableId, policyId);
                closedSlots.add(slot);
              }
            }
          }
          return 0;
        } catch (Exception e) {
          LOG.error("TMS Iceberg maintenance failed for action_ids {}", actionIds, e);
          return 1;
        } finally {
          for (TargetTable table : tables) {
            for (int i = 0; i < policyIds.size(); i++) {
              long policyId = policyIds.get(i);
              long actionId = actionIds.get(i);
              if (!closedSlots.contains(table.tableId + ":" + policyId)) {
                closeSoftSlot(client, actionId, table.tableId, policyId);
              }
            }
          }
        }
      }
    }
  }

  private static void maintainOneType(
      SparkSession spark,
      MaintenanceActionHttpClient client,
      MaintenanceType type,
      TargetTable table,
      long policyId,
      long actionId,
      Map<String, String> argsByName)
      throws Exception {
    // Orphan cleanup: no S0 row (dashboard uses procedure outcomes only).
    if (type == MaintenanceType.REMOVE_ORPHAN_FILES) {
      AfterSample after =
          runProcedure(spark, type, table.catalogName, table.tableIdentifier, argsByName, null);
      tryReportAfterMetrics(client, actionId, table.tableId, policyId, after);
      return;
    }

    BeforeSample before = sampleBefore(spark, table.catalogName, table.tableIdentifier, type);
    if (!tryReportBeforeMetrics(client, actionId, table.tableId, policyId, before)) {
      LOG.info(
          "TMS type {} stopped for action_id {} table {} policy {}: before-metrics failed"
              + " (abandoned or unbound)",
          type.wireName,
          actionId,
          table.tableId,
          policyId);
      return;
    }
    if (usesSnapshotDriftGate(type) && before.snapshotId != before.headSnapshotId) {
      LOG.info(
          "TMS type {} skipped for action_id {} table {} policy {}: snapshot drifted"
              + " (sampled={} head={})",
          type.wireName,
          actionId,
          table.tableId,
          policyId,
          before.snapshotId,
          before.headSnapshotId);
      return;
    }
    if (!MaintenanceTriggerGate.shouldMaintain(type, argsByName, before.metricsJson)) {
      LOG.info(
          "TMS type {} skipped for action_id {} table {} policy {}: trigger-expr skip",
          type.wireName,
          actionId,
          table.tableId,
          policyId);
      return;
    }
    AfterSample after =
        runProcedure(spark, type, table.catalogName, table.tableIdentifier, argsByName, before);
    tryReportAfterMetrics(client, actionId, table.tableId, policyId, after);
  }

  /**
   * Snapshot drift skip applies only to compaction and manifest rewrite (inventory-sensitive).
   * Expiry and orphan cleanup always proceed past HEAD drift.
   */
  @VisibleForTesting
  static boolean usesSnapshotDriftGate(MaintenanceType type) {
    return type == MaintenanceType.REWRITE_DATA_FILES || type == MaintenanceType.REWRITE_MANIFESTS;
  }

  private static List<TargetTable> parseTables(@Nullable String tablesJson) {
    if (tablesJson == null || tablesJson.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "tables JSON is required (array of table_id, catalog_name, table_identifier)");
    }
    try {
      List<Map<String, Object>> raw =
          JsonUtils.anyFieldMapper()
              .readValue(tablesJson, new TypeReference<List<Map<String, Object>>>() {});
      if (raw == null || raw.isEmpty()) {
        throw new IllegalArgumentException("tables must not be empty");
      }
      List<TargetTable> tables = new ArrayList<>(raw.size());
      for (Map<String, Object> entry : raw) {
        if (entry == null) {
          throw new IllegalArgumentException("tables entry must not be null");
        }
        Object tableIdObj = entry.get("table_id");
        Object catalogObj = entry.get("catalog_name");
        Object tableObj = entry.get("table_identifier");
        if (tableIdObj == null || catalogObj == null || tableObj == null) {
          throw new IllegalArgumentException(
              "tables entry requires table_id, catalog_name, table_identifier");
        }
        long tableId = Long.parseLong(String.valueOf(tableIdObj));
        if (tableId <= 0) {
          throw new IllegalArgumentException("tables table_id must be positive: " + tableId);
        }
        String catalogName = String.valueOf(catalogObj).trim();
        String tableIdentifier = String.valueOf(tableObj).trim();
        if (catalogName.isEmpty() || tableIdentifier.isEmpty()) {
          throw new IllegalArgumentException(
              "tables catalog_name and table_identifier must not be blank");
        }
        tables.add(new TargetTable(tableId, catalogName, tableIdentifier));
      }
      return tables;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("tables must be valid JSON: " + e.getMessage(), e);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("tables table_id must be a long", e);
    }
  }

  private static ImmutableList<MaintenanceType> parseMaintenanceTypes(
      @Nullable String maintenanceTypes) {
    String chain = IcebergJobUtils.nullIfUnresolvedPlaceholder(maintenanceTypes);
    if (chain == null || chain.trim().isEmpty()) {
      throw new IllegalArgumentException("maintenance_types is required");
    }
    ImmutableList.Builder<MaintenanceType> parsed = ImmutableList.builder();
    for (String part : chain.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        parsed.add(MaintenanceType.fromWire(trimmed));
      }
    }
    ImmutableList<MaintenanceType> types = parsed.build();
    if (types.isEmpty()) {
      throw new IllegalArgumentException("maintenance_types must not be empty");
    }
    return types;
  }

  private static List<Long> parseIdList(String fieldName, @Nullable String raw, int expectedCount) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
    List<Long> ids = new ArrayList<>();
    for (String part : value.split(",")) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        long id = Long.parseLong(trimmed);
        if (id <= 0) {
          throw new IllegalArgumentException(fieldName + " must be positive longs: " + trimmed);
        }
        ids.add(id);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(fieldName + " must be longs: " + trimmed, e);
      }
    }
    if (ids.size() != expectedCount) {
      throw new IllegalArgumentException(
          String.format(
              "%s count %s must match maintenance_types count %s",
              fieldName, ids.size(), expectedCount));
    }
    return ids;
  }

  private static AfterSample runProcedure(
      SparkSession spark,
      MaintenanceType type,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argsByName,
      @Nullable BeforeSample before)
      throws Exception {
    switch (type) {
      case REWRITE_DATA_FILES:
        {
          String strategy = IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("strategy"));
          if (strategy == null || strategy.isEmpty()) {
            strategy = "binpack";
          }
          String sql =
              IcebergRewriteDataFilesJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  strategy,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("sort-order")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("where")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("options")));
          LOG.info("Executing TMS rewrite_data_files: {}", sql);
          spark.sql(sql).collect();
          Map<String, Object> afterMetrics =
              collectCompactionMetrics(spark, catalogName, tableIdentifier);
          long afterSnapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
          return new AfterSample(afterSnapshotId, toMetricsJson(afterMetrics));
        }
      case REWRITE_MANIFESTS:
        {
          IcebergRewriteManifestsJob.validateUseCaching(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("use-caching")));
          IcebergRewriteManifestsJob.validateSpecId(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spec-id")));
          String sql =
              IcebergRewriteManifestsJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("use-caching")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spec-id")));
          LOG.info("Executing TMS rewrite_manifests: {}", sql);
          List<Row> results = spark.sql(sql).collectAsList();
          long rewritten = 0L;
          if (!results.isEmpty()) {
            Row result = results.get(0);
            rewritten = ((Number) result.get(0)).longValue();
            long added = ((Number) result.get(1)).longValue();
            LOG.info("Rewrite manifests results: rewritten={}, added={}", rewritten, added);
          }
          long afterSnapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
          return new AfterSample(afterSnapshotId, toMetricsJson(manifestAfterMetrics(rewritten)));
        }
      case EXPIRE_SNAPSHOTS:
        {
          String retainLast =
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("retain-last"));
          IcebergExpireSnapshotsJob.validateRetainLast(retainLast);
          boolean streamResults =
              IcebergExpireSnapshotsJob.parseStreamResults(
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("stream-results")));
          String sql =
              IcebergExpireSnapshotsJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("older-than")),
                  retainLast,
                  streamResults);
          LOG.info("Executing TMS expire_snapshots: {}", sql);
          Row[] results = (Row[]) spark.sql(sql).collect();
          if (results.length > 0) {
            LOG.info(
                "Expire snapshots deleted data/manifest/manifest-list files: {}/{}/{}",
                results[0].getLong(0),
                results[0].getLong(1),
                results[0].getLong(2));
          }
          ExpireInventory afterInventory =
              collectExpireInventory(spark, catalogName, tableIdentifier);
          ExpireInventory beforeInventory =
              before == null || before.expireInventory == null
                  ? afterInventory
                  : before.expireInventory;
          Map<String, Object> afterMetrics = expireAfterMetrics(beforeInventory, afterInventory);
          long afterSnapshotId =
              currentSnapshotIdAllowingEmpty(spark, catalogName, tableIdentifier);
          return new AfterSample(afterSnapshotId, toMetricsJson(afterMetrics));
        }
      case REMOVE_ORPHAN_FILES:
        {
          Map<String, String> orphanOpts = new HashMap<>();
          orphanOpts.put("catalog", catalogName);
          orphanOpts.put("table", tableIdentifier);
          putIfPresent(orphanOpts, "older-than", argsByName.get("older-than"));
          putIfPresent(orphanOpts, "location", argsByName.get("location"));
          putIfPresent(orphanOpts, "dry-run", argsByName.get("dry-run"));
          IcebergRemoveOrphanFilesJob.OrphanCleanupResult orphan =
              IcebergRemoveOrphanFilesJob.execute(spark, orphanOpts);
          long afterSnapshotId =
              currentSnapshotIdAllowingEmpty(spark, catalogName, tableIdentifier);
          return new AfterSample(
              afterSnapshotId,
              toMetricsJson(orphanAfterMetrics(orphan.filesRemoved(), orphan.bytesReclaimed())));
        }
      default:
        throw new IllegalStateException("Unhandled maintenance type: " + type);
    }
  }

  /**
   * Samples snapshot + metrics for the before-metrics gate. Re-reads HEAD after metrics so drift
   * can be detected for compaction / manifest rewrite.
   */
  private static BeforeSample sampleBefore(
      SparkSession spark, String catalogName, String tableIdentifier, MaintenanceType type) {
    long snapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    Map<String, Object> payload;
    ExpireInventory expireInventory = null;
    switch (type) {
      case REWRITE_DATA_FILES:
        payload = collectCompactionMetrics(spark, catalogName, tableIdentifier);
        break;
      case REWRITE_MANIFESTS:
        payload = collectManifestBeforeMetrics(spark, catalogName, tableIdentifier);
        break;
      case EXPIRE_SNAPSHOTS:
        expireInventory = collectExpireInventory(spark, catalogName, tableIdentifier);
        payload =
            expireBeforeMetrics(expireInventory.snapshotCount(), expireInventory.totalFileSize());
        break;
      case REMOVE_ORPHAN_FILES:
        throw new IllegalStateException("Orphan cleanup must not sample before-metrics");
      default:
        throw new IllegalStateException("Unhandled maintenance type: " + type);
    }
    long headSnapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    return new BeforeSample(snapshotId, headSnapshotId, toMetricsJson(payload), expireInventory);
  }

  /**
   * Compaction S0/S1 metrics from the Iceberg files metadata table (same formulas as {@link
   * IcebergUpdateStatsAndMetricsJob}). Partitioned tables use max partition MSE, sum of delete /
   * file counts and sizes, and a file-count-weighted average size.
   */
  @VisibleForTesting
  static Map<String, Object> collectCompactionMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    boolean partitioned =
        IcebergUpdateStatsAndMetricsJob.isPartitionedTable(spark, catalogName, tableIdentifier);
    if (partitioned) {
      String sql =
          IcebergUpdateStatsAndMetricsJob.buildPartitionStatsSql(catalogName, tableIdentifier);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      return aggregatePartitionCompactionMetrics(rows);
    }

    String sql = IcebergUpdateStatsAndMetricsJob.buildTableStatsSql(catalogName, tableIdentifier);
    Row[] rows = (Row[]) spark.sql(sql).collect();
    if (rows.length == 0) {
      return compactionMetrics(0D, 0L, 0D, 0L, 0L);
    }
    Row row = rows[0];
    return compactionMetrics(
        numberAsDouble(row, "data-file-mse"),
        numberAsLong(row, "delete-file-number"),
        numberAsDouble(row, "avg-file-size"),
        numberAsLong(row, "total-file-size"),
        numberAsLong(row, "file-number"));
  }

  @VisibleForTesting
  static Map<String, Object> aggregatePartitionCompactionMetrics(Row[] rows) {
    double maxMse = 0D;
    long deleteFiles = 0L;
    long fileNumber = 0L;
    long totalFileSize = 0L;
    double weightedAvgNumerator = 0D;
    for (Row row : rows) {
      maxMse = Math.max(maxMse, numberAsDouble(row, "data-file-mse"));
      deleteFiles += numberAsLong(row, "delete-file-number");
      long partitionFiles = numberAsLong(row, "file-number");
      fileNumber += partitionFiles;
      totalFileSize += numberAsLong(row, "total-file-size");
      weightedAvgNumerator += numberAsDouble(row, "avg-file-size") * partitionFiles;
    }
    double avgFileSize = fileNumber == 0L ? 0D : weightedAvgNumerator / fileNumber;
    return compactionMetrics(maxMse, deleteFiles, avgFileSize, totalFileSize, fileNumber);
  }

  @VisibleForTesting
  static Map<String, Object> compactionMetrics(
      double dataFileMse,
      long deleteFileNumber,
      double avgFileSize,
      long totalFileSize,
      long fileNumber) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, dataFileMse);
    metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, deleteFileNumber);
    metrics.put(AVG_FILE_SIZE_METRIC, avgFileSize);
    metrics.put(TOTAL_FILE_SIZE_METRIC, totalFileSize);
    metrics.put(FILE_NUMBER_METRIC, fileNumber);
    return metrics;
  }

  @VisibleForTesting
  static Map<String, Object> collectManifestBeforeMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Table table = loadIcebergTable(spark, catalogName, tableIdentifier);
    Snapshot snapshot = table.currentSnapshot();
    long count = 0L;
    long totalSize = 0L;
    if (snapshot != null) {
      for (ManifestFile manifest : snapshot.allManifests(table.io())) {
        count++;
        totalSize += manifest.length();
      }
    }
    return manifestBeforeMetrics(count, count == 0L ? 0D : (double) totalSize / count);
  }

  @VisibleForTesting
  static Map<String, Object> manifestBeforeMetrics(long manifestNumber, double avgManifestSize) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(MANIFEST_NUMBER_METRIC, manifestNumber);
    metrics.put(AVG_MANIFEST_SIZE_METRIC, avgManifestSize);
    return metrics;
  }

  @VisibleForTesting
  static Map<String, Object> manifestAfterMetrics(long manifestsRemoved) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(MANIFESTS_REMOVED_METRIC, manifestsRemoved);
    return metrics;
  }

  @VisibleForTesting
  static Map<String, Object> expireBeforeMetrics(long snapshotCount, long totalFileSize) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(SNAPSHOT_COUNT_METRIC, snapshotCount);
    metrics.put(TOTAL_FILE_SIZE_METRIC, totalFileSize);
    return metrics;
  }

  /**
   * S1 expire metrics from set difference so concurrent commits do not shrink cleared/reclaimed
   * counts: snapshots/files present in S0 but absent after expire.
   */
  @VisibleForTesting
  static Map<String, Object> expireAfterMetrics(ExpireInventory before, ExpireInventory after) {
    return expireAfterMetrics(
        snapshotsCleared(before.snapshotIds, after.snapshotIds),
        bytesReclaimed(before.fileSizesByPath, after.fileSizesByPath));
  }

  @VisibleForTesting
  static Map<String, Object> expireAfterMetrics(long snapshotsCleared, long bytesReclaimed) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(SNAPSHOTS_CLEARED_METRIC, snapshotsCleared);
    metrics.put(BYTES_RECLAIMED_METRIC, bytesReclaimed);
    return metrics;
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
   * Expire S0/S1 inventory: snapshot ids plus distinct reachable content files across history
   * ({@code all_files}), same metadata-table style as compaction's {@code files} SQL.
   */
  @VisibleForTesting
  static ExpireInventory collectExpireInventory(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Set<Long> snapshotIds = new HashSet<>();
    for (Snapshot snapshot : loadIcebergTable(spark, catalogName, tableIdentifier).snapshots()) {
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
    return new ExpireInventory(snapshotIds, fileSizesByPath);
  }

  /**
   * Distinct content files still referenced by any tracked snapshot. {@code GROUP BY file_path}
   * avoids double-counting files that appear in multiple snapshots ({@code all_files} may duplicate
   * rows).
   */
  @VisibleForTesting
  static String buildReachableContentFilesSql(String catalogName, String tableIdentifier) {
    return "SELECT file_path AS `file-path`, MAX(file_size_in_bytes) AS `file-size` FROM "
        + buildMetadataTableIdentifier(catalogName, tableIdentifier, "all_files")
        + " GROUP BY file_path";
  }

  @VisibleForTesting
  static String buildMetadataTableIdentifier(
      String catalogName, String tableIdentifier, String metadataTable) {
    String[] levels = tableIdentifier.split("\\.");
    if (levels.length != 2) {
      throw new IllegalArgumentException(
          "table_identifier must use schema.table format, but got: " + tableIdentifier);
    }
    return IcebergJobUtils.escapeSqlIdentifier(catalogName)
        + "."
        + IcebergJobUtils.escapeSqlIdentifier(levels[0])
        + "."
        + IcebergJobUtils.escapeSqlIdentifier(levels[1])
        + "."
        + IcebergJobUtils.escapeSqlIdentifier(metadataTable);
  }

  @VisibleForTesting
  static Map<String, Object> orphanAfterMetrics(long filesRemoved, long bytesReclaimed) {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(ORPHAN_FILES_REMOVED_METRIC, filesRemoved);
    metrics.put(ORPHAN_BYTES_RECLAIMED_METRIC, bytesReclaimed);
    return metrics;
  }

  private static double numberAsDouble(Row row, String field) {
    Number number = row.getAs(field);
    return number == null ? 0D : number.doubleValue();
  }

  private static long numberAsLong(Row row, String field) {
    Number number = row.getAs(field);
    return number == null ? 0L : number.longValue();
  }

  private static long currentSnapshotId(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Snapshot snapshot = loadIcebergTable(spark, catalogName, tableIdentifier).currentSnapshot();
    if (snapshot == null) {
      throw new IllegalStateException(
          "Iceberg table has no current snapshot: " + catalogName + "." + tableIdentifier);
    }
    return snapshot.snapshotId();
  }

  /** After expiry/orphan, HEAD may briefly be empty; report {@code -1} in that case. */
  private static long currentSnapshotIdAllowingEmpty(
      SparkSession spark, String catalogName, String tableIdentifier) {
    Snapshot snapshot = loadIcebergTable(spark, catalogName, tableIdentifier).currentSnapshot();
    return snapshot == null ? -1L : snapshot.snapshotId();
  }

  private static Table loadIcebergTable(
      SparkSession spark, String catalogName, String tableIdentifier) {
    // Catch Exception (not Spark NoSuchTableException) so the Gravitino server can load this class
    // without Spark catalyst on the classpath.
    try {
      return Spark3Util.loadIcebergTable(
          spark, String.format("%s.%s", catalogName, tableIdentifier));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load Iceberg table for " + catalogName + "." + tableIdentifier, e);
    }
  }

  private static void closeSoftSlot(
      MaintenanceActionHttpClient client, long actionId, long tableId, long policyId) {
    try {
      client.finish(actionId, new MaintenanceActionFinishRequest(tableId, policyId));
    } catch (Exception e) {
      LOG.warn(
          "Best-effort TMS finish failed for action_id {} table {} policy {}",
          actionId,
          tableId,
          policyId,
          e);
    }
  }

  /**
   * Reports before-metrics. Failure means the soft slot is abandoned/unbound — do not run the
   * procedure.
   */
  private static boolean tryReportBeforeMetrics(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      BeforeSample sample) {
    if (sample.metricsJson == null || sample.metricsJson.trim().isEmpty()) {
      return false;
    }
    try {
      client.reportBeforeMetrics(
          actionId,
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId, sample.metricsJson));
      return true;
    } catch (Exception e) {
      LOG.warn(
          "TMS before metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId,
          e.toString());
      return false;
    }
  }

  private static void tryReportAfterMetrics(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      AfterSample sample) {
    if (sample.metricsJson == null || sample.metricsJson.trim().isEmpty()) {
      LOG.warn(
          "TMS after metrics skipped for action_id {} table {} policy {}: empty metrics JSON",
          actionId,
          tableId,
          policyId);
      return;
    }
    try {
      client.reportAfterMetrics(
          actionId,
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId, sample.metricsJson));
    } catch (Exception e) {
      LOG.warn(
          "TMS after metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId,
          e.toString());
    }
  }

  @VisibleForTesting
  static String toMetricsJson(Map<String, Object> metrics) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(metrics);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize metrics JSON", e);
    }
  }

  private static Map<String, String> parseUpdaterOptions(@Nullable String json) {
    if (json == null || json.trim().isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      @SuppressWarnings("unchecked")
      Map<String, String> parsed = JsonUtils.anyFieldMapper().readValue(json, Map.class);
      return parsed == null ? Collections.emptyMap() : parsed;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid updater-options JSON: " + e.getMessage(), e);
    }
  }

  private static void putIfPresent(Map<String, String> target, String key, @Nullable String raw) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  private static List<String> templateArguments() {
    return Arrays.asList(
        "--maintenance-types",
        "{{maintenance_types}}",
        "--policy-ids",
        "{{policy_ids}}",
        "--action-ids",
        "{{action_ids}}",
        "--tables",
        "{{tables}}",
        "--updater-options",
        "{{updater_options:-}}",
        "--strategy",
        "{{strategy:-binpack}}",
        "--sort-order",
        "{{sort_order:-}}",
        "--where",
        "{{where_clause:-}}",
        "--options",
        "{{options:-}}",
        "--use-caching",
        "{{use_caching:-}}",
        "--spec-id",
        "{{spec_id:-}}",
        "--older-than",
        "{{older_than:-}}",
        "--retain-last",
        "{{retain_last:-}}",
        "--stream-results",
        "{{stream_results:-false}}",
        "--location",
        "{{location:-}}",
        "--dry-run",
        "{{dry_run:-false}}",
        "--spark-conf",
        "{{spark_conf:-}}",
        "--rules",
        "{{rules:-}}");
  }

  /** One table this job should maintain. */
  private static final class TargetTable {
    private final long tableId;
    private final String catalogName;
    private final String tableIdentifier;

    private TargetTable(long tableId, String catalogName, String tableIdentifier) {
      this.tableId = tableId;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
    }
  }

  /**
   * Snapshot-expiry inventory used for S0 reporting and set-difference S1. Concurrent commits only
   * add ids/paths to the after set; cleared/reclaimed count ids/paths that disappeared.
   */
  @VisibleForTesting
  static final class ExpireInventory {
    private final Set<Long> snapshotIds;
    private final Map<String, Long> fileSizesByPath;

    ExpireInventory(Set<Long> snapshotIds, Map<String, Long> fileSizesByPath) {
      this.snapshotIds = Collections.unmodifiableSet(new HashSet<>(snapshotIds));
      this.fileSizesByPath = Collections.unmodifiableMap(new HashMap<>(fileSizesByPath));
    }

    long snapshotCount() {
      return snapshotIds.size();
    }

    long totalFileSize() {
      long total = 0L;
      for (Long size : fileSizesByPath.values()) {
        total += size == null ? 0L : size;
      }
      return total;
    }
  }

  /** Before-metrics sample: snapshot at sample time, HEAD after metrics, and metrics JSON. */
  private static final class BeforeSample {
    private final long snapshotId;
    private final long headSnapshotId;
    private final String metricsJson;
    @Nullable private final ExpireInventory expireInventory;

    private BeforeSample(
        long snapshotId,
        long headSnapshotId,
        String metricsJson,
        @Nullable ExpireInventory expireInventory) {
      this.snapshotId = snapshotId;
      this.headSnapshotId = headSnapshotId;
      this.metricsJson = metricsJson;
      this.expireInventory = expireInventory;
    }
  }

  /** After-metrics sample for S1 reporting. */
  private static final class AfterSample {
    private final long snapshotId;
    private final String metricsJson;

    private AfterSample(long snapshotId, String metricsJson) {
      this.snapshotId = snapshotId;
      this.metricsJson = metricsJson;
    }
  }
}
