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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TMS Spark job: sample before-metrics → gate → run Iceberg procedure → close soft slot.
 *
 * <p>Started only from the built-in template (cron or commit-chain). {@code maintenance_types} is
 * one type or a comma-separated chain; {@code policy_ids} / {@code action_ids} align 1:1 with those
 * types. Soft-slot HTTP callbacks use {@code action_ids}.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /** Built-in template name used by TMS submit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  // Bump when template behavior / args / class wiring changes (required by built-in registration).
  private static final String VERSION = "v1";

  /** JobConf: one wire name, or comma-separated commit chain. */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /** JobConf: JSON array of {@code table_id}, {@code catalog_name}, {@code table_identifier}. */
  public static final String TABLES_KEY = "tables";

  /** JobConf: policy ids parallel to {@link #MAINTENANCE_TYPES_KEY}. */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /** JobConf: soft-slot action ids parallel to {@link #POLICY_IDS_KEY}. */
  public static final String ACTION_IDS_KEY = "action_ids";

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
        .withComment("TMS Iceberg maintenance (before-metrics gate, procedure, finish soft slot)")
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
                BeforeSample before =
                    sampleBefore(spark, table.catalogName, table.tableIdentifier, type);
                if (!tryReportBeforeMetrics(client, actionId, table.tableId, policyId, before)) {
                  LOG.info(
                      "TMS type {} stopped for action_id {} table {} policy {}: before-metrics"
                          + " failed (abandoned or unbound)",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId);
                  continue;
                }
                if (before.snapshotId != before.headSnapshotId) {
                  LOG.info(
                      "TMS type {} skipped for action_id {} table {} policy {}: snapshot drifted"
                          + " (sampled={} head={})",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId,
                      before.snapshotId,
                      before.headSnapshotId);
                  continue;
                }
                if (!MaintenanceTriggerGate.shouldMaintain(type, argsByName, before.metricsJson)) {
                  LOG.info(
                      "TMS type {} skipped for action_id {} table {} policy {}: trigger-expr skip",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId);
                  continue;
                }
                runProcedure(spark, type, table.catalogName, table.tableIdentifier, argsByName);
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

  private static void runProcedure(
      SparkSession spark,
      MaintenanceType type,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argsByName)
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
          break;
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
          spark.sql(sql).collect();
          break;
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
          break;
        }
      case REMOVE_ORPHAN_FILES:
        {
          Map<String, String> orphanOpts = new HashMap<>();
          orphanOpts.put("catalog", catalogName);
          orphanOpts.put("table", tableIdentifier);
          putIfPresent(orphanOpts, "older-than", argsByName.get("older-than"));
          putIfPresent(orphanOpts, "location", argsByName.get("location"));
          putIfPresent(orphanOpts, "dry-run", argsByName.get("dry-run"));
          IcebergRemoveOrphanFilesJob.execute(spark, orphanOpts);
          break;
        }
      default:
        throw new IllegalStateException("Unhandled maintenance type: " + type);
    }
  }

  /**
   * Samples snapshot + metrics for the before-metrics gate. Re-reads HEAD after metrics so drift
   * can be detected.
   */
  private static BeforeSample sampleBefore(
      SparkSession spark, String catalogName, String tableIdentifier, MaintenanceType type) {
    long snapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    Map<String, Object> payload = new HashMap<>();
    payload.put("maintenanceType", type.wireName());
    payload.put("snapshotId", snapshotId);
    if (type == MaintenanceType.REWRITE_DATA_FILES) {
      payload.putAll(collectCompactionMetrics(spark, catalogName, tableIdentifier));
    }
    long headSnapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    try {
      return new BeforeSample(
          snapshotId, headSnapshotId, JsonUtils.anyFieldMapper().writeValueAsString(payload));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize metrics JSON", e);
    }
  }

  /**
   * Compaction metrics from the Iceberg files metadata table (same formulas as {@link
   * IcebergUpdateStatsAndMetricsJob}). Partitioned tables use max partition MSE and sum of delete
   * files so any hot partition can trip {@code trigger-expr}.
   */
  private static Map<String, Object> collectCompactionMetrics(
      SparkSession spark, String catalogName, String tableIdentifier) {
    boolean partitioned =
        IcebergUpdateStatsAndMetricsJob.isPartitionedTable(spark, catalogName, tableIdentifier);
    if (partitioned) {
      String sql =
          IcebergUpdateStatsAndMetricsJob.buildPartitionStatsSql(catalogName, tableIdentifier);
      Row[] rows = (Row[]) spark.sql(sql).collect();
      Map<String, Object> metrics = new HashMap<>();
      double maxMse = 0D;
      long deleteFiles = 0L;
      for (Row row : rows) {
        maxMse = Math.max(maxMse, numberAsDouble(row, "data-file-mse"));
        deleteFiles += numberAsLong(row, "delete-file-number");
      }
      metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, maxMse);
      metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, deleteFiles);
      return metrics;
    }

    String sql = IcebergUpdateStatsAndMetricsJob.buildTableStatsSql(catalogName, tableIdentifier);
    Row[] rows = (Row[]) spark.sql(sql).collect();
    Map<String, Object> metrics = new HashMap<>();
    if (rows.length == 0) {
      metrics.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 0D);
      metrics.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
      return metrics;
    }
    Row row = rows[0];
    metrics.put(
        IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, numberAsDouble(row, "data-file-mse"));
    metrics.put(
        IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC,
        numberAsLong(row, "delete-file-number"));
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
    // Catch Exception (not Spark NoSuchTableException) so the Gravitino server can load this class
    // without Spark catalyst on the classpath.
    try {
      Table table =
          Spark3Util.loadIcebergTable(spark, String.format("%s.%s", catalogName, tableIdentifier));
      Snapshot snapshot = table.currentSnapshot();
      if (snapshot == null) {
        throw new IllegalStateException(
            "Iceberg table has no current snapshot: " + catalogName + "." + tableIdentifier);
      }
      return snapshot.snapshotId();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load Iceberg snapshot for " + catalogName + "." + tableIdentifier, e);
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

  /** Before-metrics sample: snapshot at sample time, HEAD after metrics, and metrics JSON. */
  private static final class BeforeSample {
    private final long snapshotId;
    private final long headSnapshotId;
    private final String metricsJson;

    private BeforeSample(long snapshotId, long headSnapshotId, String metricsJson) {
      this.snapshotId = snapshotId;
      this.headSnapshotId = headSnapshotId;
      this.metricsJson = metricsJson;
    }
  }
}
