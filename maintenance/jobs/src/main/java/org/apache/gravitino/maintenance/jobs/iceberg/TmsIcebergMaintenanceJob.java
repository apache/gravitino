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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.maintenance.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoAuthSettings;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
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
 * TMS Spark entry for cron (§5.2.2) and commit-chain (§5.2.3) submits.
 *
 * <p>Dispatches by {@code --maintenance-type} (single type) or {@code --maintenance-types}
 * (comma-separated commit chain). Reuses procedure SQL builders from the four standalone {@code
 * Iceberg*Job} classes. Those standalone templates stay unchanged for CLI / optimizer use.
 *
 * <p>Commit-chain runs share one occupancy claim ({@code policy_id} = first type). Only the first
 * type uses the HTTP sample/recommend gate; later types skip the HTTP gate, end early on local
 * snapshot drift, then continue the chain.
 */
public class TmsIcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(TmsIcebergMaintenanceJob.class);

  /** Built-in template name submitted by TMS cron / commit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /** JobConf / CLI key for the maintenance kind ({@code maintenance_type}). */
  public static final String MAINTENANCE_TYPE_KEY = "maintenance_type";

  /** JobConf / CLI key for a commit-chain of wire types ({@code maintenance_types}). */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  private static final String VERSION = "v1";

  /** Supported {@code maintenance_type} values. */
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
     * @return wire value used in jobConf / CLI
     */
    public String wireName() {
      return wireName;
    }

    /**
     * @param raw jobConf or CLI value
     * @return parsed type
     */
    public static MaintenanceType fromWire(@Nullable String raw) {
      if (raw == null || raw.trim().isEmpty()) {
        throw new IllegalArgumentException("maintenance_type is required");
      }
      String normalized = raw.trim().toLowerCase(Locale.ROOT);
      for (MaintenanceType type : values()) {
        if (type.wireName.equals(normalized)) {
          return type;
        }
      }
      throw new IllegalArgumentException(
          "Unknown maintenance_type '"
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
        .withExecutable(resolveExecutable(TmsIcebergMaintenanceJob.class))
        .withClassName(TmsIcebergMaintenanceJob.class.getName())
        .withArguments(buildArguments())
        .withConfigs(IcebergSparkConfigUtils.buildTemplateSparkConfigs())
        .withCustomFields(
            Collections.singletonMap(JobTemplateProvider.PROPERTY_VERSION_KEY, VERSION))
        .build();
  }

  /**
   * TMS cron / commit entry point.
   *
   * @param args named arguments from the job template
   */
  public static void main(String[] args) {
    int code = run(args);
    if (code != 0) {
      System.exit(code);
    }
  }

  @VisibleForTesting
  static int run(String[] args) {
    Map<String, String> argMap = IcebergJobUtils.parseArguments(args);
    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");
    if (catalogName == null || tableIdentifier == null) {
      LOG.error("--catalog and --table are required");
      return 1;
    }

    List<MaintenanceType> types;
    long jobId;
    long tableId;
    long policyId;
    try {
      types =
          parseMaintenanceTypes(argMap.get("maintenance-types"), argMap.get("maintenance-type"));
      jobId = requirePositiveLong(argMap.get("gravitino-job-id"), "gravitino-job-id");
      tableId = requirePositiveLong(argMap.get("tms-table-id"), "tms-table-id");
      policyId = requirePositiveLong(argMap.get("tms-policy-id"), "tms-policy-id");
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid TMS maintenance arguments: {}", e.getMessage());
      return 1;
    }

    Map<String, String> updaterOptions =
        parseUpdaterOptions(
            IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("updater-options")));

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
      try (GravitinoClient client = openClient(updaterOptions)) {
        Long afterSnapshotId = null;
        String finishMetrics = null;
        boolean shouldFinish = false;
        try {
          for (int i = 0; i < types.size(); i++) {
            MaintenanceType type = types.get(i);
            boolean httpGate = i == 0;
            SnapshotSample s0 = sampleSnapshot(spark, catalogName, tableIdentifier, type);
            afterSnapshotId = s0.snapshotId;
            finishMetrics = s0.metricsJson;

            if (httpGate) {
              MaintenanceActionSampleResponse gate =
                  client.reportMaintenanceActionSample(
                      jobId,
                      new MaintenanceActionSampleRequest(
                          tableId, policyId, s0.snapshotId, s0.headSnapshotId, s0.metricsJson));
              if (!gate.isContinueMaintenance()) {
                LOG.info(
                    "TMS gate stopped job {} type={} reason={}; continuing chain if any",
                    jobId,
                    type.wireName,
                    gate.getStopReason());
                continue;
              }
            } else if (s0.snapshotId != s0.headSnapshotId) {
              // End this type on local drift; continue with the next type in the same Job.
              LOG.info(
                  "TMS commit-chain type {} skipped for job {} due to snapshot drift (s0={} head={})",
                  type.wireName,
                  jobId,
                  s0.snapshotId,
                  s0.headSnapshotId);
              continue;
            }

            runProcedure(spark, type, catalogName, tableIdentifier, argMap);
            SnapshotSample s1 = sampleSnapshot(spark, catalogName, tableIdentifier, type);
            afterSnapshotId = s1.snapshotId;
            finishMetrics = s1.metricsJson;
          }
          // Finish only on a clean exit (including gate-stop / drift-skip). Procedure failures
          // leave the action open so OccupancyGate can reclaim when the Gravitino job is terminal.
          shouldFinish = true;
          return 0;
        } catch (Exception e) {
          LOG.error("TMS Iceberg maintenance failed for job {}", jobId, e);
          return 1;
        } finally {
          // Finish before try-with-resources closes the client.
          if (shouldFinish) {
            finishQuietly(client, jobId, afterSnapshotId, finishMetrics);
          }
        }
      }
    }
  }

  /**
   * @param maintenanceTypes comma-separated wire names from commit-chain submit (may be blank)
   * @param maintenanceType single wire name (cron path)
   * @return ordered types to run
   */
  @VisibleForTesting
  static ImmutableList<MaintenanceType> parseMaintenanceTypes(
      @Nullable String maintenanceTypes, @Nullable String maintenanceType) {
    String chain = IcebergJobUtils.nullIfUnresolvedPlaceholder(maintenanceTypes);
    if (chain != null && !chain.trim().isEmpty()) {
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
    return ImmutableList.of(MaintenanceType.fromWire(maintenanceType));
  }

  @VisibleForTesting
  static void runProcedure(
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

  @VisibleForTesting
  static SnapshotSample sampleSnapshot(
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
    return new SnapshotSample(snapshotId, headSnapshotId, toMetricsJson(payload));
  }

  /**
   * Compaction metrics from the Iceberg files metadata table (same formulas as {@link
   * IcebergUpdateStatsAndMetricsJob}). For partitioned tables, uses max partition MSE and sum of
   * delete-file counts so any hot partition can trip {@code trigger-expr}.
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
    if (rows.length == 0) {
      Map<String, Object> empty = new HashMap<>();
      empty.put(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC, 0D);
      empty.put(IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC, 0L);
      return empty;
    }
    Row row = rows[0];
    Map<String, Object> metrics = new HashMap<>();
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

  @VisibleForTesting
  static long currentSnapshotId(SparkSession spark, String catalogName, String tableIdentifier) {
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

  @VisibleForTesting
  static String stubMetricsJson(String maintenanceType, long snapshotId) {
    Map<String, Object> payload = new HashMap<>();
    payload.put("maintenanceType", maintenanceType);
    payload.put("snapshotId", snapshotId);
    return toMetricsJson(payload);
  }

  private static String toMetricsJson(Map<String, Object> payload) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize metrics JSON", e);
    }
  }

  private static void finishQuietly(
      GravitinoClient client,
      long jobId,
      @Nullable Long afterSnapshotId,
      @Nullable String metricsJson) {
    try {
      client.finishMaintenanceAction(
          jobId, new MaintenanceActionFinishRequest(afterSnapshotId, metricsJson));
    } catch (Exception e) {
      LOG.warn("Best-effort TMS finish failed for job {} (control plane will reclaim)", jobId, e);
    }
  }

  private static GravitinoClient openClient(Map<String, String> updaterOptions) {
    Map<String, String> optimizerProperties = buildOptimizerProperties(updaterOptions);
    requireGravitinoConfig(optimizerProperties);
    return GravitinoClientUtils.createClient(
        new OptimizerEnv(new OptimizerConfig(optimizerProperties)), updaterOptions);
  }

  @VisibleForTesting
  static Map<String, String> buildOptimizerProperties(Map<String, String> updaterOptions) {
    Map<String, String> optimizerProperties = new HashMap<>(updaterOptions);
    Optional<String> gravitinoUri =
        firstNonEmpty(
            updaterOptions.get("gravitino_uri"),
            updaterOptions.get("gravitino-uri"),
            updaterOptions.get(OptimizerConfig.GRAVITINO_URI));
    Optional<String> metalake =
        firstNonEmpty(
            updaterOptions.get("metalake"), updaterOptions.get(OptimizerConfig.GRAVITINO_METALAKE));
    gravitinoUri.ifPresent(uri -> optimizerProperties.put(OptimizerConfig.GRAVITINO_URI, uri));
    metalake.ifPresent(value -> optimizerProperties.put(OptimizerConfig.GRAVITINO_METALAKE, value));
    GravitinoAuthSettings.copyAliases(optimizerProperties);
    return optimizerProperties;
  }

  private static void requireGravitinoConfig(Map<String, String> optimizerProperties) {
    String gravitinoUri = optimizerProperties.get(OptimizerConfig.GRAVITINO_URI);
    String metalake = optimizerProperties.get(OptimizerConfig.GRAVITINO_METALAKE);
    Preconditions.checkArgument(
        gravitinoUri != null && !gravitinoUri.trim().isEmpty(),
        "updater_options must contain gravitino_uri");
    Preconditions.checkArgument(
        metalake != null && !metalake.trim().isEmpty(), "updater_options must contain metalake");
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

  private static long requirePositiveLong(@Nullable String raw, String name) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("--" + name + " is required");
    }
    try {
      long parsed = Long.parseLong(value.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException("--" + name + " must be positive");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("--" + name + " must be a long: " + value, e);
    }
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

  @SafeVarargs
  private static Optional<String> firstNonEmpty(String... values) {
    for (String value : values) {
      if (value != null && !value.trim().isEmpty()) {
        return Optional.of(value.trim());
      }
    }
    return Optional.empty();
  }

  private static List<String> buildArguments() {
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--maintenance-type",
        "{{maintenance_type}}",
        "--maintenance-types",
        "{{maintenance_types:-}}",
        "--gravitino-job-id",
        "{{gravitino_job_id}}",
        "--tms-table-id",
        "{{tms_table_id}}",
        "--tms-policy-id",
        "{{tms_policy_id}}",
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
        "{{spark_conf:-}}");
  }

  @VisibleForTesting
  static final class SnapshotSample {
    final long snapshotId;
    final long headSnapshotId;
    final String metricsJson;

    SnapshotSample(long snapshotId, long headSnapshotId, String metricsJson) {
      this.snapshotId = snapshotId;
      this.headSnapshotId = headSnapshotId;
      this.metricsJson = metricsJson;
    }
  }
}
