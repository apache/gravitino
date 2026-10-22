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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.job.JobHandle;
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
 * TMS Spark entry for cron (§5.2.2) and commit-chain (§5.2.3) submits.
 *
 * <p>Dispatches by {@code --maintenance-types}: one wire name, or a comma-separated commit chain.
 * Reuses procedure SQL builders from the four standalone {@code Iceberg*Job} classes. Those
 * standalone templates stay unchanged for CLI / optimizer use.
 *
 * <p>Commit-chain runs share one occupancy claim ({@code policy_id} = first type). Continue / stop
 * (snapshot drift) is decided locally in the job. Snapshot metrics may be reported best-effort via
 * the TMS metrics callback; occupancy is released when the Gravitino job reaches a terminal state.
 *
 * <p>Template parameters mirror other Iceberg built-ins: {@code maintenance_types}, {@code
 * updater_options} (Gravitino URI / metalake / auth, same name as update-stats), plus catalog /
 * table / procedure options. A batch may pass {@code tables} (JSON array) so one Spark job covers
 * multiple tables under the same Gravitino job id. The Gravitino job id is read from the local
 * executor staging directory name ({@code job-} plus the numeric id), or from {@link
 * #GRAVITINO_JOB_ID_ENV} when that directory is not the process working directory.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /**
   * Optional job-id override. Not injected by {@code JobManager}; the local executor staging
   * directory is the default source.
   */
  @VisibleForTesting static final String GRAVITINO_JOB_ID_ENV = "GRAVITINO_JOB_ID";

  /** Built-in template name submitted by TMS cron / commit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /**
   * JobConf / CLI key for procedure dispatch ({@code maintenance_types}). One wire name, or a
   * comma-separated commit chain. Required; no template default.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /** JobConf / CLI key for a single-table run ({@code table_id}). */
  public static final String TABLE_ID_KEY = "table_id";

  /**
   * JobConf / CLI key for a multi-table batch ({@code tables}): JSON array of {@code table_id},
   * {@code catalog_name}, {@code table_identifier}.
   */
  public static final String TABLES_KEY = "tables";

  private static final String VERSION = "v1";

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

    List<MaintenanceType> types;
    long jobId;
    List<TableTarget> tables;
    try {
      types = parseMaintenanceTypes(argMap.get("maintenance-types"));
      jobId = resolveGravitinoJobId();
      tables = resolveTableTargets(argMap);
    } catch (IllegalArgumentException | IllegalStateException e) {
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
      try (MaintenanceActionHttpClient client =
          MaintenanceActionHttpClient.fromUpdaterOptions(updaterOptions)) {
        try {
          for (TableTarget table : tables) {
            SnapshotSample lastBefore = null;
            SnapshotSample lastAfter = null;
            for (MaintenanceType type : types) {
              SnapshotSample s0 =
                  sampleSnapshot(spark, table.catalogName, table.tableIdentifier, type);
              lastBefore = s0;
              reportBeforeQuietly(client, jobId, table.tableId, s0, /*finish=*/ false);

              if (s0.snapshotId != s0.headSnapshotId) {
                LOG.info(
                    "TMS type {} skipped for job {} table {} due to snapshot drift (s0={} head={})",
                    type.wireName,
                    jobId,
                    table.tableId,
                    s0.snapshotId,
                    s0.headSnapshotId);
                continue;
              }

              runProcedure(spark, type, table.catalogName, table.tableIdentifier, argMap);
              SnapshotSample s1 =
                  sampleSnapshot(spark, table.catalogName, table.tableIdentifier, type);
              lastAfter = s1;
              reportAfterQuietly(client, jobId, table.tableId, s1, /*finish=*/ false);
            }
            // Close soft slot without re-writing metrics (finish API).
            if (lastAfter != null) {
              finishQuietly(client, jobId, table.tableId, lastAfter.snapshotId);
            } else if (lastBefore != null) {
              finishQuietly(client, jobId, table.tableId, /*afterSnapshotId=*/ null);
            } else {
              LOG.warn(
                  "TMS job {} table {} ended without a metrics sample; soft slot left open",
                  jobId,
                  table.tableId);
            }
          }
          return 0;
        } catch (Exception e) {
          LOG.error("TMS Iceberg maintenance failed for job {}", jobId, e);
          return 1;
        }
      }
    }
  }

  /**
   * Resolves one or more table targets. Prefers {@code --tables} JSON when present; otherwise
   * requires {@code --catalog}, {@code --table}, and {@code --table-id}.
   */
  @VisibleForTesting
  static List<TableTarget> resolveTableTargets(Map<String, String> argMap) {
    String tablesJson = IcebergJobUtils.nullIfUnresolvedPlaceholder(argMap.get("tables"));
    if (tablesJson != null && !tablesJson.trim().isEmpty()) {
      return parseTables(tablesJson);
    }
    String catalogName = argMap.get("catalog");
    String tableIdentifier = argMap.get("table");
    if (catalogName == null || tableIdentifier == null) {
      throw new IllegalArgumentException(
          "--catalog and --table are required when --tables is omitted");
    }
    long tableId = parsePositiveLongArg(argMap.get("table-id"), "table-id");
    return ImmutableList.of(new TableTarget(tableId, catalogName, tableIdentifier));
  }

  @VisibleForTesting
  static List<TableTarget> parseTables(String tablesJson) {
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

  /**
   * @param maintenanceTypes one wire name, or a comma-separated commit chain
   * @return ordered types to run
   */
  @VisibleForTesting
  static ImmutableList<MaintenanceType> parseMaintenanceTypes(@Nullable String maintenanceTypes) {
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

  private static void reportBeforeQuietly(
      MaintenanceActionHttpClient client,
      long jobId,
      long tableId,
      SnapshotSample sample,
      boolean finish) {
    reportMetricsQuietly(client, jobId, tableId, sample, finish, /*after=*/ false);
  }

  private static void reportAfterQuietly(
      MaintenanceActionHttpClient client,
      long jobId,
      long tableId,
      SnapshotSample sample,
      boolean finish) {
    reportMetricsQuietly(client, jobId, tableId, sample, finish, /*after=*/ true);
  }

  private static void finishQuietly(
      MaintenanceActionHttpClient client,
      long jobId,
      long tableId,
      @Nullable Long afterSnapshotId) {
    try {
      client.finish(jobId, new MaintenanceActionFinishRequest(tableId, afterSnapshotId));
    } catch (Exception e) {
      LOG.warn(
          "Best-effort TMS finish failed for job {} table {} afterSnapshotId={}",
          jobId,
          tableId,
          afterSnapshotId,
          e);
    }
  }

  private static void reportMetricsQuietly(
      MaintenanceActionHttpClient client,
      long jobId,
      long tableId,
      SnapshotSample sample,
      boolean finish,
      boolean after) {
    if (sample == null || sample.metricsJson == null || sample.metricsJson.trim().isEmpty()) {
      return;
    }
    try {
      MaintenanceActionMetricsRequest request =
          new MaintenanceActionMetricsRequest(
              tableId, sample.snapshotId, sample.metricsJson, finish);
      if (after) {
        client.reportAfterMetrics(jobId, request);
      } else {
        client.reportBeforeMetrics(jobId, request);
      }
    } catch (Exception e) {
      LOG.warn(
          "Best-effort TMS {} metrics report failed for job {} table {} snapshot {} finish={}",
          after ? "after" : "before",
          jobId,
          tableId,
          sample.snapshotId,
          finish,
          e);
    }
  }

  private static long parsePositiveLongArg(@Nullable String raw, String argName) {
    String value = IcebergJobUtils.nullIfUnresolvedPlaceholder(raw);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("--" + argName + " is required");
    }
    try {
      long parsed = Long.parseLong(value.trim());
      if (parsed <= 0) {
        throw new IllegalArgumentException("--" + argName + " must be a positive long: " + value);
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("--" + argName + " must be a long: " + value, e);
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

  /**
   * Resolves the Gravitino job id for TMS callbacks.
   *
   * <p>Prefers {@link #GRAVITINO_JOB_ID_ENV}. Falls back to parsing a {@code job-} plus numeric id
   * segment from {@code user.dir} (local executor staging directory).
   *
   * @return numeric Gravitino job id
   */
  @VisibleForTesting
  static long resolveGravitinoJobId() {
    return resolveGravitinoJobId(
        System.getenv(GRAVITINO_JOB_ID_ENV), System.getProperty("user.dir"));
  }

  @VisibleForTesting
  static long resolveGravitinoJobId(@Nullable String envValue, @Nullable String userDir) {
    if (envValue != null && !envValue.trim().isEmpty()) {
      try {
        long parsed = Long.parseLong(envValue.trim());
        if (parsed <= 0) {
          throw new IllegalStateException(
              GRAVITINO_JOB_ID_ENV + " must be a positive long: " + envValue);
        }
        return parsed;
      } catch (NumberFormatException e) {
        throw new IllegalStateException(GRAVITINO_JOB_ID_ENV + " must be a long: " + envValue, e);
      }
    }
    if (userDir != null && !userDir.trim().isEmpty()) {
      Path dir = Paths.get(userDir).toAbsolutePath().normalize();
      for (Path current = dir; current != null; current = current.getParent()) {
        Path fileName = current.getFileName();
        if (fileName == null) {
          continue;
        }
        String name = fileName.toString();
        if (name.startsWith(JobHandle.JOB_ID_PREFIX)) {
          String idPart = name.substring(JobHandle.JOB_ID_PREFIX.length());
          try {
            long parsed = Long.parseLong(idPart);
            if (parsed > 0) {
              return parsed;
            }
          } catch (NumberFormatException ignored) {
            // Keep walking parents; staging dirs use job-<numericId>.
          }
        }
      }
    }
    throw new IllegalStateException(
        "Cannot resolve Gravitino job id: set "
            + GRAVITINO_JOB_ID_ENV
            + " or run under a job-<id> staging directory");
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
    return Arrays.asList(
        "--catalog",
        "{{catalog_name}}",
        "--table",
        "{{table_identifier}}",
        "--maintenance-types",
        "{{maintenance_types}}",
        "--table-id",
        "{{table_id:-}}",
        "--tables",
        "{{tables:-}}",
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
  static final class TableTarget {
    final long tableId;
    final String catalogName;
    final String tableIdentifier;

    TableTarget(long tableId, String catalogName, String tableIdentifier) {
      this.tableId = tableId;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
    }
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
