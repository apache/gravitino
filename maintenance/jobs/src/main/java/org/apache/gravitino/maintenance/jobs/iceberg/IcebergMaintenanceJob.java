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
 * TMS Spark entry for cron (§5.2.2) and commit-chain (§5.2.3) {@code runJob} submits.
 *
 * <p>Not a user-facing CLI. Operators / optimizer keep using the four standalone {@code
 * Iceberg*Job} templates. This class is launched only via the built-in job template; program
 * arguments are template-expanded {@code jobConf} values.
 *
 * <p>Dispatches by {@code maintenance_types} (one wire name or a comma-separated commit chain).
 * Reuses procedure SQL builders from the standalone {@code Iceberg*Job} classes. Continue / stop
 * (snapshot drift and trigger-expr recommend) is decided in-job after S0 sample. Soft slots are
 * closed via the finish callback in {@code finally}.
 *
 * <p>TMS-specific {@code jobConf} keys: {@code maintenance_types}, {@code policy_ids} and {@code
 * action_ids} (parallel comma-separated ids), {@code tables} (JSON targets with system-filled
 * {@code table_id}), {@code updater_options}, {@code rules}. Shared procedure / Spark keys match
 * other Iceberg built-ins. Soft-slot callbacks use injected {@code action_ids}, not the Gravitino
 * job run id.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /** Built-in template name submitted by TMS cron / commit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /**
   * JobConf key for procedure dispatch ({@code maintenance_types}). One wire name, or a
   * comma-separated commit chain. Required; no template default.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /**
   * JobConf key for table targets ({@code tables}): JSON array of {@code table_id}, {@code
   * catalog_name}, {@code table_identifier}. Single- and multi-table TMS submits use the same
   * shape; {@code table_id} is system-filled by enqueue/submit.
   */
  public static final String TABLES_KEY = "tables";

  /**
   * JobConf key for policy entity ids parallel to {@link #MAINTENANCE_TYPES_KEY} (comma-separated).
   * Required so metrics/finish callbacks can attribute soft slots when one job binds multiple
   * policies.
   */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /**
   * JobConf key for {@code table_maintenance_action.action_id} values parallel to {@link
   * #POLICY_IDS_KEY} (comma-separated). Injected by TMS submit after soft-slot acquire.
   */
  public static final String ACTION_IDS_KEY = "action_ids";

  /** Supported {@code maintenance_types} wire values. */
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
     * @return wire value used in jobConf
     */
    public String wireName() {
      return wireName;
    }

    /**
     * @param raw jobConf {@code maintenance_types} segment
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
            "TMS Iceberg maintenance: S0 sample/gate, procedure dispatch (single or commit-chain),"
                + " S1 finish")
        .withExecutable(resolveExecutable(IcebergMaintenanceJob.class))
        .withClassName(IcebergMaintenanceJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .build();
  }

  /**
   * Spark application entry used by the job executor after template expansion. Not a supported
   * operator CLI.
   *
   * @param args template-expanded jobConf as named arguments
   */
  public static void main(String[] args) {
    int code = run(args);
    if (code != 0) {
      System.exit(code);
    }
  }

  /**
   * @param args template-expanded jobConf as named arguments
   * @return process exit code ({@code 0} success, {@code 1} failure)
   */
  static int run(String[] args) {
    Map<String, String> argMap = IcebergJobUtils.parseArguments(args);

    List<MaintenanceType> types;
    List<Long> policyIds;
    List<Long> actionIds;
    List<TableTarget> tables;
    Map<String, String> updaterOptions;
    try {
      types = parseMaintenanceTypes(argMap.get("maintenance-types"));
      policyIds = parsePolicyIds(argMap.get("policy-ids"), types.size());
      actionIds = parseActionIds(argMap.get("action-ids"), types.size());
      tables = parseTables(IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("tables")));
      updaterOptions =
          parseUpdaterOptions(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("updater-options")));
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
    String sparkConfJson = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spark-conf"));
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson).forEach(sparkBuilder::config);
    }

    try (SparkSession spark = sparkBuilder.getOrCreate()) {
      IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);
      try (MaintenanceActionHttpClient client =
          MaintenanceActionHttpClient.fromUpdaterOptions(updaterOptions)) {
        // Soft slots are per action_id; close every slot even when a procedure fails or we never
        // reach a later type/table.
        Set<String> finishedSlots = new HashSet<>();
        try {
          for (TableTarget table : tables) {
            for (int i = 0; i < types.size(); i++) {
              MaintenanceType type = types.get(i);
              long policyId = policyIds.get(i);
              long actionId = actionIds.get(i);
              try {
                SnapshotSample s0 =
                    sampleSnapshot(spark, table.catalogName, table.tableIdentifier, type);
                if (!reportBeforeMetrics(client, actionId, table.tableId, policyId, s0)) {
                  LOG.info(
                      "TMS type {} stopped for action_id {} table {} policy {}:"
                          + " before-metrics claim failed (abandoned or unbound)",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId);
                  continue;
                }

                if (s0.snapshotId != s0.headSnapshotId) {
                  LOG.info(
                      "TMS type {} skipped for action_id {} table {} policy {} due to snapshot drift"
                          + " (s0={} head={})",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId,
                      s0.snapshotId,
                      s0.headSnapshotId);
                  continue;
                }

                if (!MaintenanceTriggerGate.shouldMaintain(type, argMap, s0.metricsJson)) {
                  LOG.info(
                      "TMS type {} skipped for action_id {} table {} policy {} due to in-Spark"
                          + " recommend skip",
                      type.wireName,
                      actionId,
                      table.tableId,
                      policyId);
                  continue;
                }

                runProcedure(spark, type, table.catalogName, table.tableIdentifier, argMap);
                SnapshotSample s1 =
                    sampleSnapshot(spark, table.catalogName, table.tableIdentifier, type);
                reportMetricsQuietly(
                    client, actionId, table.tableId, policyId, s1, /*after=*/ true);
              } finally {
                finishQuietly(client, actionId, table.tableId, policyId);
                finishedSlots.add(slotKey(table.tableId, policyId));
              }
            }
          }
          return 0;
        } catch (Exception e) {
          LOG.error("TMS Iceberg maintenance failed for action_ids {}", actionIds, e);
          return 1;
        } finally {
          for (TableTarget table : tables) {
            for (int i = 0; i < policyIds.size(); i++) {
              long policyId = policyIds.get(i);
              long actionId = actionIds.get(i);
              if (!finishedSlots.contains(slotKey(table.tableId, policyId))) {
                finishQuietly(client, actionId, table.tableId, policyId);
              }
            }
          }
        }
      }
    }
  }

  private static List<TableTarget> parseTables(@Nullable String tablesJson) {
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
      List<TableTarget> tables = new ArrayList<>(raw.size());
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
        tables.add(new TableTarget(tableId, catalogName, tableIdentifier));
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

  private static List<Long> parsePolicyIds(@Nullable String policyIds, int expectedCount) {
    String raw = IcebergJobUtils.nullIfUnresolvedPlaceholder(policyIds);
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalArgumentException("policy_ids is required");
    }
    List<Long> parsed = new ArrayList<>();
    for (String part : raw.split(",")) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        long policyId = Long.parseLong(trimmed);
        if (policyId <= 0) {
          throw new IllegalArgumentException("policy_ids must be positive longs: " + trimmed);
        }
        parsed.add(policyId);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("policy_ids must be longs: " + trimmed, e);
      }
    }
    if (parsed.size() != expectedCount) {
      throw new IllegalArgumentException(
          String.format(
              "policy_ids count %s must match maintenance_types count %s",
              parsed.size(), expectedCount));
    }
    return parsed;
  }

  private static String slotKey(long tableId, long policyId) {
    return tableId + ":" + policyId;
  }

  private static void runProcedure(
      SparkSession spark,
      MaintenanceType type,
      String catalogName,
      String tableIdentifier,
      Map<String, String> argMap)
      throws Exception {
    switch (type) {
      case REWRITE_DATA_FILES:
        {
          String strategy = firstNonBlank(argMap.get("strategy"), "binpack");
          String sql =
              IcebergRewriteDataFilesJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  strategy,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("sort-order")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("where")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("options")));
          LOG.info("Executing TMS rewrite_data_files: {}", sql);
          spark.sql(sql).collect();
          break;
        }
      case REWRITE_MANIFESTS:
        {
          IcebergRewriteManifestsJob.validateUseCaching(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("use-caching")));
          IcebergRewriteManifestsJob.validateSpecId(
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spec-id")));
          String sql =
              IcebergRewriteManifestsJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("use-caching")),
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("spec-id")));
          LOG.info("Executing TMS rewrite_manifests: {}", sql);
          spark.sql(sql).collect();
          break;
        }
      case EXPIRE_SNAPSHOTS:
        {
          String retainLast =
              IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("retain-last"));
          IcebergExpireSnapshotsJob.validateRetainLast(retainLast);
          boolean streamResults =
              IcebergExpireSnapshotsJob.parseStreamResults(
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("stream-results")));
          String sql =
              IcebergExpireSnapshotsJob.buildProcedureCall(
                  catalogName,
                  tableIdentifier,
                  IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("older-than")),
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
          putIfPresent(orphanOpts, "older-than", argMap.get("older-than"));
          putIfPresent(orphanOpts, "location", argMap.get("location"));
          putIfPresent(orphanOpts, "dry-run", argMap.get("dry-run"));
          IcebergRemoveOrphanFilesJob.execute(spark, orphanOpts);
          break;
        }
      default:
        throw new IllegalStateException("Unhandled maintenance type: " + type);
    }
  }

  private static SnapshotSample sampleSnapshot(
      SparkSession spark, String catalogName, String tableIdentifier, MaintenanceType type) {
    // Capture S0 first, then collect metrics (may take time), then re-read HEAD for the drift gate.
    long snapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    Map<String, Object> payload = new HashMap<>();
    payload.put("maintenanceType", type.wireName());
    payload.put("snapshotId", snapshotId);
    if (type == MaintenanceType.REWRITE_DATA_FILES) {
      payload.putAll(collectCompactionMetrics(spark, catalogName, tableIdentifier));
    }
    long headSnapshotId = currentSnapshotId(spark, catalogName, tableIdentifier);
    try {
      return new SnapshotSample(
          snapshotId, headSnapshotId, JsonUtils.anyFieldMapper().writeValueAsString(payload));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize metrics JSON", e);
    }
  }

  /**
   * Compaction metrics from the Iceberg files metadata table (same formulas as {@link
   * IcebergUpdateStatsAndMetricsJob}). For partitioned tables, uses max partition MSE and sum of
   * delete-file counts so any hot partition can trip {@code trigger-expr}.
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
    // Catch Exception (not Spark NoSuchTableException) so BuiltInJobTemplateProvider can load this
    // class on the Gravitino server classpath, which does not ship Spark catalyst types.
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

  private static void finishQuietly(
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
   * S0 metrics are a hard gate: failure means the soft slot is abandoned/unbound, so maintenance
   * must not run.
   */
  private static boolean reportBeforeMetrics(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      SnapshotSample sample) {
    if (sample == null || sample.metricsJson == null || sample.metricsJson.trim().isEmpty()) {
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

  private static void reportMetricsQuietly(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      SnapshotSample sample,
      boolean after) {
    if (sample == null || sample.metricsJson == null || sample.metricsJson.trim().isEmpty()) {
      return;
    }
    try {
      MaintenanceActionMetricsRequest request =
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId, sample.metricsJson);
      if (after) {
        client.reportAfterMetrics(actionId, request);
      } else {
        client.reportBeforeMetrics(actionId, request);
      }
    } catch (Exception e) {
      LOG.warn(
          "Best-effort TMS {} metrics report failed for action_id {} table {} policy {} snapshot {}",
          after ? "after" : "before",
          actionId,
          tableId,
          policyId,
          sample.snapshotId,
          e);
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

  private static List<Long> parseActionIds(@Nullable String raw, int expectedCount) {
    if (raw == null || raw.trim().isEmpty()) {
      throw new IllegalArgumentException("action_ids is required");
    }
    String[] parts = raw.split(",");
    List<Long> ids = new ArrayList<>(parts.length);
    for (String part : parts) {
      String trimmed = part.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        long parsed = Long.parseLong(trimmed);
        if (parsed <= 0) {
          throw new IllegalArgumentException("action_ids must be positive longs: " + trimmed);
        }
        ids.add(parsed);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("action_ids must be longs: " + trimmed, e);
      }
    }
    if (ids.size() != expectedCount) {
      throw new IllegalArgumentException(
          String.format(
              "action_ids count %s must match maintenance_types count %s",
              ids.size(), expectedCount));
    }
    return ids;
  }

  private static void putIfPresent(Map<String, String> target, String key, @Nullable String raw) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  private static String firstNonBlank(@Nullable String value, String defaultValue) {
    String normalized = IcebergJobUtils.nullIfUnresolvedPlaceholder(value);
    return normalized == null || normalized.isEmpty() ? defaultValue : normalized;
  }

  private static List<String> buildArguments() {
    // Named args are the Spark job-executor wiring for expanded jobConf — not a user CLI.
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

  private static final class TableTarget {
    private final long tableId;
    private final String catalogName;
    private final String tableIdentifier;

    private TableTarget(long tableId, String catalogName, String tableIdentifier) {
      this.tableId = tableId;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
    }
  }

  private static final class SnapshotSample {
    private final long snapshotId;
    private final long headSnapshotId;
    private final String metricsJson;

    private SnapshotSample(long snapshotId, long headSnapshotId, String metricsJson) {
      this.snapshotId = snapshotId;
      this.headSnapshotId = headSnapshotId;
      this.metricsJson = metricsJson;
    }
  }
}
