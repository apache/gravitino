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
import java.util.EnumMap;
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
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TMS Spark job: collect before-metrics → gate → execute procedure → report after-metrics → finish
 * the maintenance action.
 *
 * <p>Started only from the built-in template (cron or commit-chain). {@code maintenance_types} is
 * one type or a comma-separated chain; {@code policy_ids} / {@code action_ids} align 1:1 with those
 * types. {@code action_ids} identify the TMS actions this job must finish when done.
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
  private static final String VERSION = "v1";

  /** JobConf: one wire name, or comma-separated commit chain. */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /** JobConf: JSON array of {@code table_id}, {@code catalog_name}, {@code table_identifier}. */
  public static final String TABLES_KEY = "tables";

  /** JobConf: policy ids parallel to {@link #MAINTENANCE_TYPES_KEY}. */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /** JobConf: TMS action ids parallel to {@link #POLICY_IDS_KEY}. */
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
     * @return the built-in job that implements this maintenance type
     */
    public IcebergMaintenanceAction action() {
      return ACTIONS.get(this);
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

  private static final Map<MaintenanceType, IcebergMaintenanceAction> ACTIONS = createActions();

  private static Map<MaintenanceType, IcebergMaintenanceAction> createActions() {
    Map<MaintenanceType, IcebergMaintenanceAction> actions = new EnumMap<>(MaintenanceType.class);
    actions.put(MaintenanceType.REWRITE_DATA_FILES, new IcebergRewriteDataFilesJob());
    actions.put(MaintenanceType.REWRITE_MANIFESTS, new IcebergRewriteManifestsJob());
    actions.put(MaintenanceType.EXPIRE_SNAPSHOTS, new IcebergExpireSnapshotsJob());
    actions.put(MaintenanceType.REMOVE_ORPHAN_FILES, new IcebergRemoveOrphanFilesJob());
    return Collections.unmodifiableMap(actions);
  }

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "TMS Iceberg maintenance (before metrics, gate, procedure, after metrics, finish)")
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
        // TMS opens one action per (table, policy) before this job runs; finish each when done.
        Set<String> finishedTablePolicyKeys = new HashSet<>();
        try {
          for (TargetTable table : tables) {
            for (int i = 0; i < types.size(); i++) {
              MaintenanceType type = types.get(i);
              long policyId = policyIds.get(i);
              long actionId = actionIds.get(i);
              String tablePolicyKey = table.tableId + ":" + policyId;
              try {
                runMaintenanceForType(spark, client, type, table, policyId, actionId, argsByName);
              } finally {
                finishMaintenanceAction(client, actionId, table.tableId, policyId);
                finishedTablePolicyKeys.add(tablePolicyKey);
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
              String tablePolicyKey = table.tableId + ":" + policyId;
              if (!finishedTablePolicyKeys.contains(tablePolicyKey)) {
                finishMaintenanceAction(client, actionIds.get(i), table.tableId, policyId);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Shared S0 → gate → procedure → S1 pipeline. Types without S0 (orphan cleanup) skip the before
   * half and still report after-metrics.
   */
  private static void runMaintenanceForType(
      SparkSession spark,
      MaintenanceActionHttpClient client,
      MaintenanceType type,
      TargetTable table,
      long policyId,
      long actionId,
      Map<String, String> argsByName)
      throws Exception {
    IcebergMaintenanceAction action = type.action();
    TableSnapshotMetrics before = null;
    if (action.hasBeforeMetrics()) {
      before = action.collectBeforeMetrics(spark, table.catalogName, table.tableIdentifier);
      if (!reportBeforeMetricsOrStop(client, actionId, table.tableId, policyId, before)) {
        LOG.info(
            "TMS type {} stopped for action_id {} table {} policy {}: before-metrics failed"
                + " (abandoned or unbound)",
            type.wireName,
            actionId,
            table.tableId,
            policyId);
        return;
      }
      long headSnapshotId =
          IcebergMaintenanceSnapshots.requireCurrentId(
              spark, table.catalogName, table.tableIdentifier);
      if (action.shouldCheckSnapshotDrift() && before.snapshotId() != headSnapshotId) {
        LOG.info(
            "TMS type {} skipped for action_id {} table {} policy {}: snapshot drifted"
                + " (sampled={} head={})",
            type.wireName,
            actionId,
            table.tableId,
            policyId,
            before.snapshotId(),
            headSnapshotId);
        return;
      }
      if (!MaintenanceTriggerGate.shouldMaintain(type, argsByName, before.metricsJson())) {
        LOG.info(
            "TMS type {} skipped for action_id {} table {} policy {}: trigger-expr skip",
            type.wireName,
            actionId,
            table.tableId,
            policyId);
        return;
      }
    }

    TableSnapshotMetrics after =
        action.executeAndCollectAfter(
            spark, table.catalogName, table.tableIdentifier, argsByName, before);
    reportAfterMetrics(client, actionId, table.tableId, policyId, after);
  }

  /**
   * Snapshot drift skip applies only to compaction and manifest rewrite (inventory-sensitive).
   * Expiry and orphan cleanup always proceed past HEAD drift.
   */
  @VisibleForTesting
  static boolean shouldCheckSnapshotDrift(MaintenanceType type) {
    return type.action().shouldCheckSnapshotDrift();
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

  private static void finishMaintenanceAction(
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
   * Reports before-metrics. Failure means the TMS action is abandoned/unbound — do not run the
   * procedure.
   */
  private static boolean reportBeforeMetricsOrStop(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      TableSnapshotMetrics sample) {
    if (sample.metricsJson() == null || sample.metricsJson().trim().isEmpty()) {
      return false;
    }
    try {
      client.reportBeforeMetrics(
          actionId,
          new MaintenanceActionMetricsRequest(
              tableId, policyId, sample.snapshotId(), sample.metricsJson()));
      return true;
    } catch (Exception e) {
      LOG.warn(
          "TMS before metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId(),
          e.toString());
      return false;
    }
  }

  private static void reportAfterMetrics(
      MaintenanceActionHttpClient client,
      long actionId,
      long tableId,
      long policyId,
      TableSnapshotMetrics sample) {
    if (sample.metricsJson() == null || sample.metricsJson().trim().isEmpty()) {
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
              tableId, policyId, sample.snapshotId(), sample.metricsJson()));
    } catch (Exception e) {
      LOG.warn(
          "TMS after metrics failed for action_id {} table {} policy {} snapshot {}: {}",
          actionId,
          tableId,
          policyId,
          sample.snapshotId(),
          e.toString());
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
}
