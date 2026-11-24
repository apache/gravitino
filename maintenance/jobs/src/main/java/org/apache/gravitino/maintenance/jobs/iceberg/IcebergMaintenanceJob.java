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
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TMS Spark job: collect before-metrics → gate → execute procedure → report after-metrics → finish
 * the maintenance action.
 *
 * <p>Started only from the built-in template (cron or commit-chain). JobConf carries a {@code
 * tasks} JSON array: each element is one work item with {@code table_id}, {@code policy_type}
 * ({@link Policy.BuiltInType#policyType()}), {@code policy_id}, {@code action_id}, {@code params}
 * (including {@code catalog_name}/{@code table_identifier} plus procedure flags), and gate {@code
 * rules}. Cron snapshot-expiry batches and commit chains share this shape.
 *
 * <p>{@code metricsJson} carries only metric fields (no policy type / {@code snapshotId}; those
 * live on the action/policy and {@code table_snapshot_metrics.snapshot_id}). Snapshot drift skips
 * only compaction and manifest rewrite. Orphan cleanup reports after-metrics only.
 */
public class IcebergMaintenanceJob implements BuiltInJob {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

  /** Built-in template name used by TMS submit. */
  public static final String NAME =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /** Bump when template behavior / args / class wiring changes. */
  private static final String VERSION = "v1";

  /**
   * Maps schedule/jobConf underscore param keys onto the hyphenated names {@link
   * IcebergMaintenanceAction} reads from {@code argsByName}.
   */
  private static final Map<String, String> PARAM_KEY_TO_ARG =
      ImmutableMap.<String, String>builder()
          .put("strategy", "strategy")
          .put("sort_order", "sort-order")
          .put("where_clause", "where")
          .put("options", "options")
          .put("use_caching", "use-caching")
          .put("spec_id", "spec-id")
          .put("older_than", "older-than")
          .put("retain_last", "retain-last")
          .put("stream_results", "stream-results")
          .put("location", "location")
          .put("dry_run", "dry-run")
          .build();

  @Override
  public SparkJobTemplate jobTemplate() {
    return SparkJobTemplate.builder()
        .withName(NAME)
        .withComment(
            "TMS Iceberg maintenance (before metrics, gate, procedure, after metrics, finish)")
        .withExecutable(resolveExecutable(IcebergMaintenanceJob.class))
        .withClassName(IcebergMaintenanceJob.class.getName())
        .withArguments(
            Arrays.asList(
                "--tasks",
                "{{tasks}}",
                "--updater-options",
                "{{updater_options:-}}",
                "--spark-conf",
                "{{spark_conf:-}}"))
        // Session Spark settings come from jobConf spark_conf / --spark-conf, not template configs.
        .withConfigs(Collections.emptyMap())
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

    List<MaintenanceTask> tasks;
    Map<String, String> updaterOptions;
    try {
      tasks = parseTasks(IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("tasks")));
      String updaterJson =
          IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("updater-options"));
      updaterOptions = IcebergSparkConfigUtils.parseFlatJsonMap(updaterJson, "updater-options");
    } catch (IllegalArgumentException | IllegalStateException e) {
      LOG.error("Invalid TMS maintenance arguments: {}", e.getMessage());
      return 1;
    }

    SparkSession.Builder sparkBuilder =
        SparkSession.builder().appName("Gravitino Iceberg Maintenance Task");
    String sparkConfJson =
        IcebergJobUtils.nullIfUnresolvedPlaceholder(argsByName.get("spark-conf"));
    if (sparkConfJson != null && !sparkConfJson.isEmpty()) {
      Map<String, String> sparkConfigs =
          new HashMap<>(IcebergJobUtils.parseCustomSparkConfigs(sparkConfJson));
      // Session defaultCatalog follows the first target table's catalog (single-catalog TMS path).
      IcebergSparkConfigUtils.ensureIcebergCatalogConfigs(sparkConfigs, tasks.get(0).catalogName);
      sparkConfigs.forEach(sparkBuilder::config);
    }

    try (SparkSession spark = sparkBuilder.getOrCreate()) {
      IcebergJobUtils.requireIcebergSparkRuntimeOrExit(spark);
      try (MaintenanceActionHttpClient client =
          MaintenanceActionHttpClient.fromUpdaterOptions(updaterOptions)) {
        Set<String> finishedKeys = new HashSet<>();
        boolean anyFailed = false;
        try {
          // One bad task must not abort peers in the same merged Spark job.
          for (MaintenanceTask task : tasks) {
            String key = task.tableId + ":" + task.policyId + ":" + task.actionId;
            try {
              try {
                runMaintenanceForTask(spark, client, task);
              } catch (Exception e) {
                anyFailed = true;
                LOG.error(
                    "TMS Iceberg maintenance failed for action_id {} table {} policy {}",
                    task.actionId,
                    task.tableId,
                    task.policyId,
                    e);
              }
            } finally {
              try {
                client.finish(
                    task.actionId, new MaintenanceActionFinishRequest(task.tableId, task.policyId));
              } catch (Exception e) {
                LOG.warn(
                    "Best-effort TMS finish failed for action_id {} table {} policy {}",
                    task.actionId,
                    task.tableId,
                    task.policyId,
                    e);
              }
              finishedKeys.add(key);
            }
          }
        } finally {
          for (MaintenanceTask task : tasks) {
            String key = task.tableId + ":" + task.policyId + ":" + task.actionId;
            if (!finishedKeys.contains(key)) {
              try {
                client.finish(
                    task.actionId, new MaintenanceActionFinishRequest(task.tableId, task.policyId));
              } catch (Exception finishError) {
                LOG.warn(
                    "Best-effort TMS finish failed for action_id {} table {} policy {}",
                    task.actionId,
                    task.tableId,
                    task.policyId,
                    finishError);
              }
            }
          }
        }
        return anyFailed ? 1 : 0;
      }
    }
  }

  /**
   * Shared S0 → gate → procedure → S1 pipeline. Types without S0 (orphan cleanup) skip the before
   * half; expiry / orphan always pass the in-job gate, then report after-metrics when the procedure
   * runs.
   *
   * <p>Early returns (before-metrics failure, snapshot drift, trigger skip) still {@code finish}
   * the soft-slot in the caller {@code finally} so the action does not stay in-flight;
   * after-metrics are only reported when the procedure actually runs.
   */
  private static void runMaintenanceForTask(
      SparkSession spark, MaintenanceActionHttpClient client, MaintenanceTask task)
      throws Exception {
    // Rewrite-manifests may temporarily pin spec-id after S0; restore so the next table/type
    // does not inherit that resolution.
    String priorSpecId = task.procedureArgs.get("spec-id");
    try {
      IcebergMaintenanceAction action;
      switch (task.policyType) {
        case ICEBERG_COMPACTION:
          action = new IcebergRewriteDataFilesJob();
          break;
        case ICEBERG_REWRITE_MANIFESTS:
          action = new IcebergRewriteManifestsJob();
          break;
        case ICEBERG_SNAPSHOT_EXPIRATION:
          action = new IcebergExpireSnapshotsJob();
          break;
        case ICEBERG_ORPHAN_FILE_REMOVAL:
          action = new IcebergRemoveOrphanFilesJob();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported maintenance policy type: " + task.policyType.policyType());
      }

      TableSnapshotMetrics before = null;
      if (action.hasBeforeMetrics()) {
        before =
            action.collectBeforeMetrics(
                spark, task.catalogName, task.tableIdentifier, task.procedureArgs);
        if (before.metricsJson() == null || before.metricsJson().trim().isEmpty()) {
          LOG.info(
              "TMS type {} stopped for action_id {} table {} policy {}: before-metrics failed"
                  + " (abandoned or unbound)",
              task.policyType.policyType(),
              task.actionId,
              task.tableId,
              task.policyId);
          return;
        }
        before = withCompactionDataMseRatio(before, task.policyType, task.rules);
        try {
          client.reportBeforeMetrics(
              task.actionId,
              new MaintenanceActionMetricsRequest(
                  task.tableId, task.policyId, before.snapshotId(), before.metricsJson()));
        } catch (Exception e) {
          LOG.warn(
              "TMS before metrics failed for action_id {} table {} policy {} snapshot {}: {}",
              task.actionId,
              task.tableId,
              task.policyId,
              before.snapshotId(),
              e.toString());
          LOG.info(
              "TMS type {} stopped for action_id {} table {} policy {}: before-metrics failed"
                  + " (abandoned or unbound)",
              task.policyType.policyType(),
              task.actionId,
              task.tableId,
              task.policyId);
          return;
        }
        long headSnapshotId =
            IcebergJobUtils.requireCurrentSnapshotId(spark, task.catalogName, task.tableIdentifier);
        if (action.shouldCheckSnapshotDrift() && before.snapshotId() != headSnapshotId) {
          LOG.info(
              "TMS type {} skipped for action_id {} table {} policy {}: snapshot drifted"
                  + " (sampled={} head={})",
              task.policyType.policyType(),
              task.actionId,
              task.tableId,
              task.policyId,
              before.snapshotId(),
              headSnapshotId);
          return;
        }
      }

      String metricsJson = before == null ? null : before.metricsJson();
      if (!MaintenanceTriggerGate.shouldMaintain(task.policyType, task.rules, metricsJson)) {
        LOG.info(
            "TMS type {} skipped for action_id {} table {} policy {}: trigger-expr skip",
            task.policyType.policyType(),
            task.actionId,
            task.tableId,
            task.policyId);
        return;
      }

      TableSnapshotMetrics after =
          action.executeAndCollectAfter(
              spark, task.catalogName, task.tableIdentifier, task.procedureArgs, before);
      if (after.metricsJson() == null || after.metricsJson().trim().isEmpty()) {
        LOG.warn(
            "TMS after metrics skipped for action_id {} table {} policy {}: empty metrics JSON",
            task.actionId,
            task.tableId,
            task.policyId);
        return;
      }
      after = withCompactionDataMseRatio(after, task.policyType, task.rules);
      try {
        client.reportAfterMetrics(
            task.actionId,
            new MaintenanceActionMetricsRequest(
                task.tableId, task.policyId, after.snapshotId(), after.metricsJson()));
      } catch (Exception e) {
        LOG.warn(
            "TMS after metrics failed for action_id {} table {} policy {} snapshot {}: {}",
            task.actionId,
            task.tableId,
            task.policyId,
            after.snapshotId(),
            e.toString());
      }
    } finally {
      if (priorSpecId == null) {
        task.procedureArgs.remove("spec-id");
      } else {
        task.procedureArgs.put("spec-id", priorSpecId);
      }
    }
  }

  private static List<MaintenanceTask> parseTasks(@Nullable String tasksJson) {
    if (tasksJson == null || tasksJson.trim().isEmpty()) {
      throw new IllegalArgumentException("tasks JSON is required (non-empty array)");
    }
    try {
      List<Map<String, Object>> raw =
          JsonUtils.anyFieldMapper()
              .readValue(tasksJson, new TypeReference<List<Map<String, Object>>>() {});
      if (raw == null || raw.isEmpty()) {
        throw new IllegalArgumentException("tasks must not be empty");
      }
      List<MaintenanceTask> tasks = new ArrayList<>(raw.size());
      for (Map<String, Object> entry : raw) {
        if (entry == null) {
          throw new IllegalArgumentException("tasks entry must not be null");
        }
        long tableId = requirePositiveLong(entry.get("table_id"), "table_id");
        Object policyTypeRaw = entry.get("policy_type");
        if (policyTypeRaw == null || String.valueOf(policyTypeRaw).trim().isEmpty()) {
          throw new IllegalArgumentException("policy_type is required");
        }
        Policy.BuiltInType policyType =
            Policy.BuiltInType.fromPolicyType(String.valueOf(policyTypeRaw).trim());
        switch (policyType) {
          case ICEBERG_COMPACTION:
          case ICEBERG_REWRITE_MANIFESTS:
          case ICEBERG_SNAPSHOT_EXPIRATION:
          case ICEBERG_ORPHAN_FILE_REMOVAL:
            break;
          default:
            throw new IllegalArgumentException(
                "Unsupported maintenance policy type: " + policyType.policyType());
        }
        long policyId = requirePositiveLong(entry.get("policy_id"), "policy_id");
        long actionId = requirePositiveLong(entry.get("action_id"), "action_id");

        Map<String, String> params = new LinkedHashMap<>();
        Object paramsRaw = entry.get("params");
        if (paramsRaw != null) {
          if (!(paramsRaw instanceof Map)) {
            throw new IllegalArgumentException("params must be a JSON object");
          }
          for (Map.Entry<?, ?> e : ((Map<?, ?>) paramsRaw).entrySet()) {
            if (e.getKey() != null && e.getValue() != null) {
              params.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
            }
          }
        }
        String catalogName = requireNonBlank(params.get("catalog_name"), "params.catalog_name");
        String tableIdentifier =
            requireNonBlank(params.get("table_identifier"), "params.table_identifier");

        Map<String, String> procedureArgs = new HashMap<>();
        for (Map.Entry<String, String> e : params.entrySet()) {
          String key = e.getKey();
          if (key == null
              || "catalog_name".equals(key)
              || "table_identifier".equals(key)
              || e.getValue() == null
              || e.getValue().trim().isEmpty()) {
            continue;
          }
          String argKey = PARAM_KEY_TO_ARG.get(key);
          procedureArgs.put(argKey != null ? argKey : key.replace('_', '-'), e.getValue());
        }

        Map<String, Object> rules = new LinkedHashMap<>();
        Object rulesRaw = entry.get("rules");
        if (rulesRaw != null) {
          if (!(rulesRaw instanceof Map)) {
            throw new IllegalArgumentException("rules must be a JSON object");
          }
          for (Map.Entry<?, ?> e : ((Map<?, ?>) rulesRaw).entrySet()) {
            if (e.getKey() != null) {
              rules.put(String.valueOf(e.getKey()), e.getValue());
            }
          }
        }

        tasks.add(
            new MaintenanceTask(
                tableId,
                catalogName,
                tableIdentifier,
                policyType,
                policyId,
                actionId,
                procedureArgs,
                rules));
      }
      return tasks;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("tasks must be valid JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Adds compaction Fragmentation display metric {@code custom-data-mse-ratio} from measured MSE
   * and policy {@code minDataFileMse}. No-op for other types. Does not persist the threshold
   * itself.
   */
  static TableSnapshotMetrics withCompactionDataMseRatio(
      TableSnapshotMetrics sample,
      Policy.BuiltInType policyType,
      @Nullable Map<String, Object> rules) {
    if (policyType != Policy.BuiltInType.ICEBERG_COMPACTION
        || sample == null
        || sample.metricsJson() == null
        || sample.metricsJson().trim().isEmpty()) {
      return sample;
    }
    try {
      Map<String, Object> metrics =
          JsonUtils.anyFieldMapper()
              .readValue(sample.metricsJson(), new TypeReference<Map<String, Object>>() {});
      if (metrics == null) {
        return sample;
      }
      Object mseRaw = metrics.get(IcebergDataCompactionContent.DATA_FILE_MSE_METRIC);
      if (!(mseRaw instanceof Number)) {
        return sample;
      }
      long threshold = compactionMinDataFileMse(rules);
      double ratio = threshold > 0L ? ((Number) mseRaw).doubleValue() / (double) threshold : 0D;
      metrics.put(IcebergDataCompactionContent.DATA_MSE_RATIO_METRIC, ratio);
      return TableSnapshotMetrics.of(sample.snapshotId(), metrics);
    } catch (IOException | RuntimeException e) {
      LOG.warn(
          "Failed to add {}: {}", IcebergDataCompactionContent.DATA_MSE_RATIO_METRIC, e.toString());
      return sample;
    }
  }

  static long compactionMinDataFileMse(@Nullable Map<String, Object> rules) {
    Map<String, Object> effective =
        rules == null || rules.isEmpty() ? PolicyContents.icebergDataCompaction().rules() : rules;
    Object raw = effective.get(IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY);
    if (raw instanceof Number) {
      return ((Number) raw).longValue();
    }
    return IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE;
  }

  private static long requirePositiveLong(@Nullable Object value, String field) {
    if (value == null) {
      throw new IllegalArgumentException(field + " is required");
    }
    try {
      long id = Long.parseLong(String.valueOf(value));
      if (id <= 0) {
        throw new IllegalArgumentException(field + " must be positive: " + id);
      }
      return id;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(field + " must be a long", e);
    }
  }

  private static String requireNonBlank(@Nullable Object value, String field) {
    if (value == null) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    String text = String.valueOf(value).trim();
    if (text.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return text;
  }

  /** One ordered maintenance work item inside this Spark job. */
  private static final class MaintenanceTask {
    final long tableId;
    final String catalogName;
    final String tableIdentifier;
    final Policy.BuiltInType policyType;
    final long policyId;
    final long actionId;
    final Map<String, String> procedureArgs;
    final Map<String, Object> rules;

    private MaintenanceTask(
        long tableId,
        String catalogName,
        String tableIdentifier,
        Policy.BuiltInType policyType,
        long policyId,
        long actionId,
        Map<String, String> procedureArgs,
        Map<String, Object> rules) {
      this.tableId = tableId;
      this.catalogName = catalogName;
      this.tableIdentifier = tableIdentifier;
      this.policyType = policyType;
      this.policyId = policyId;
      this.actionId = actionId;
      this.procedureArgs = procedureArgs;
      this.rules = rules;
    }
  }
}
