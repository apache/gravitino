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
package org.apache.gravitino.maintenance.scheduler.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Builds schedule-time configuration for TMS Iceberg maintenance: per-task procedure {@code
 * parameters} plus shared {@code updater_options} / {@code spark_conf}.
 *
 * <p>Policy {@code jobOptions} (including overlay from {@code gravitino.maintenance.ircAuth.*})
 * keep short IRC keys such as {@code rest.auth.type}; those are expanded to {@code
 * spark.sql.catalog.<catalog>.*} only when writing {@code spark_conf}. Dot-form Spark resource keys
 * ({@code spark.executor.memory}) land in {@code spark_conf} (never as catalog properties).
 *
 * <p>{@link #build} stores cluster {@code gravitinoAuth} / {@code ircAuth} as conf <em>keys</em>
 * (not secret values). Call {@link #resolveForSubmit} immediately before {@code runJob} to expand
 * those keys from the live {@code gravitino.conf}. Call {@link #applyActionIdsFromClaims} to stamp
 * soft-slot {@code action_id} onto {@code tasks[]} right before submit.
 */
public final class MaintenanceJobConfBuilder {

  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  /** Procedure / CALL flags for schedule {@code Task.parameters} (later {@code tasks[].params}). */
  private static final Set<String> PROCEDURE_PARAMETER_KEYS =
      ImmutableSet.of(
          "strategy",
          "sort_order",
          "where_clause",
          "options",
          "use_caching",
          "spec_id",
          "older_than",
          "retain_last",
          "stream_results",
          "location",
          "dry_run");

  private final Config gravitinoConfig;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  public MaintenanceJobConfBuilder(Config gravitinoConfig) {
    this.gravitinoConfig = gravitinoConfig;
  }

  /**
   * Schedule-time parts for one policy: procedure {@code parameters}, shared updater options, and
   * nested spark conf.
   *
   * @param policy effective maintenance policy
   * @param catalogName Spark catalog alias for IRC property scoping
   * @return schedule conf parts (factory adds catalog/table identity onto parameters)
   */
  public ScheduleConf build(PolicyEntity policy, String catalogName) {
    PolicyContent content = policy.content();
    PolicyExecutionSpec spec = executionSpec(content);
    Map<String, String> jobOptions =
        MaintenanceAuthConfigs.overlayJobOptions(
            spec.jobOptions(), MaintenanceAuthConfigs.loadIrcAuthAsConfigKeys(gravitinoConfig));
    Map<String, String> updaterOptions =
        ensureUpdaterCallbackOptions(
            policy,
            MaintenanceAuthConfigs.overlayUpdaterOptions(
                spec.updaterOptions(),
                MaintenanceAuthConfigs.loadGravitinoAuthAsConfigKeys(gravitinoConfig)));

    Map<String, String> parameters = new LinkedHashMap<>();
    Map<String, String> sparkConf = new LinkedHashMap<>();

    String existingSparkConfJson = jobOptions.get("spark_conf");
    if (existingSparkConfJson != null && !existingSparkConfJson.trim().isEmpty()) {
      sparkConf.putAll(
          IcebergSparkConfigUtils.parseFlatJsonMap(existingSparkConfJson, "spark_conf"));
    }

    for (Map.Entry<String, String> entry : jobOptions.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null
          || value == null
          || "spark_conf".equals(key)
          || "updater_options".equals(key)) {
        continue;
      }
      if (IcebergSnapshotExpirationContent.OLDER_THAN_DAYS_KEY.equals(key)) {
        long days = parsePositiveDays(value, IcebergSnapshotExpirationContent.OLDER_THAN_DAYS_KEY);
        IcebergSnapshotExpirationContent.validateOlderThanDays(days);
        parameters.put("older_than", Long.toString(days));
        continue;
      }
      routeJobOption(parameters, sparkConf, catalogName, key, value);
    }
    // Same priority as update-stats: flat jobOptions win over nested spark_conf.
    IcebergSparkConfigUtils.overlayFlatResourceAndCatalogKeys(sparkConf, jobOptions, catalogName);

    putTypedProcedureParams(parameters, content);
    ensureIcebergCatalogImpl(sparkConf, catalogName);

    if (MaintenanceJobTemplates.isIcebergMaintenance(policy.policyType()) && content != null) {
      String policyType = policy.policyType().policyType();
      Map<String, Map<String, Object>> rulesByType = new LinkedHashMap<>();
      rulesByType.put(policyType, new LinkedHashMap<>(content.rules()));
      parameters.put(
          MaintenanceJobTemplates.RULES_KEY,
          writeJson(rulesByType, MaintenanceJobTemplates.RULES_KEY));
    }

    String updaterJson = writeJson(updaterOptions, "updater_options");
    String sparkConfJson =
        sparkConf.isEmpty() ? null : writeJson(new TreeMap<>(sparkConf), "spark_conf");
    return new ScheduleConf(parameters, updaterJson, sparkConfJson);
  }

  private static void ensureIcebergCatalogImpl(Map<String, String> sparkConf, String catalogName) {
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;
    if (sparkConf.containsKey(catalogPrefix + ".type")
        || sparkConf.containsKey(catalogPrefix + ".uri")
        || sparkConf.containsKey(catalogPrefix + ".warehouse")) {
      sparkConf.putIfAbsent(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog");
      sparkConf.putIfAbsent(
          "spark.sql.extensions", IcebergSparkConfigUtils.ICEBERG_SPARK_EXTENSIONS);
    }
  }

  /**
   * Expands {@code gravitino.maintenance.*} conf-key references in a schedule snapshot into live
   * values from {@code gravitino.conf}. Call immediately before {@code runJob}.
   *
   * @param scheduleJobConf jobConf from submit projection / {@code task_data}
   * @return new map with secrets and URIs resolved
   */
  public Map<String, String> resolveForSubmit(Map<String, String> scheduleJobConf) {
    Map<String, String> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : scheduleJobConf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY.equals(key)
          || "spark_conf".equals(key)) {
        resolved.put(key, resolveJsonMapValues(value, key));
      } else if (MaintenanceJobTemplates.TASKS_KEY.equals(key)) {
        resolved.put(key, resolveTasksJson(value));
      } else {
        resolved.put(key, MaintenanceAuthConfigs.resolveConfigReference(gravitinoConfig, value));
      }
    }
    return resolved;
  }

  /**
   * Returns a copy of {@code jobConf} with each {@code tasks[]} element stamped with {@code
   * action_id} from {@code claims} (same order). Call after {@link #resolveForSubmit} and before
   * {@code runJob}.
   *
   * @param jobConf resolved runJob map whose {@code tasks[]} lack {@code action_id}
   * @param claims soft-slot claims parallel to {@code tasks[]}
   * @return new map with {@code action_id} applied
   */
  public static Map<String, String> applyActionIdsFromClaims(
      Map<String, String> jobConf, List<OccupancyClaim> claims) {
    Preconditions.checkNotNull(jobConf, "jobConf");
    Preconditions.checkArgument(claims != null && !claims.isEmpty(), "claims must be non-empty");
    String tasksJson = jobConf.get(MaintenanceJobTemplates.TASKS_KEY);
    Preconditions.checkArgument(
        tasksJson != null && !tasksJson.isBlank(),
        "jobConf.%s is required",
        MaintenanceJobTemplates.TASKS_KEY);
    try {
      JsonNode root = JsonUtils.anyFieldMapper().readTree(tasksJson);
      Preconditions.checkArgument(
          root != null && root.isArray(),
          "jobConf.%s must be a JSON array",
          MaintenanceJobTemplates.TASKS_KEY);
      ArrayNode tasks = (ArrayNode) root;
      Preconditions.checkArgument(
          tasks.size() == claims.size(),
          "jobConf tasks size %s must match claims size %s",
          tasks.size(),
          claims.size());
      for (int i = 0; i < tasks.size(); i++) {
        JsonNode task = tasks.get(i);
        Preconditions.checkArgument(
            task != null && task.isObject(), "tasks[%s] must be a JSON object", i);
        long actionId = claims.get(i).actionId();
        Preconditions.checkArgument(
            actionId > 0, "claims[%s].actionId must be positive, got %s", i, actionId);
        ((ObjectNode) task).put(MaintenanceJobTemplates.ACTION_ID_KEY, actionId);
      }
      Map<String, String> out = new LinkedHashMap<>(jobConf);
      out.put(
          MaintenanceJobTemplates.TASKS_KEY, JsonUtils.anyFieldMapper().writeValueAsString(tasks));
      return out;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to apply action_id onto jobConf." + MaintenanceJobTemplates.TASKS_KEY, e);
    }
  }

  private String resolveJsonMapValues(String json, String field) {
    Map<String, String> parsed = IcebergSparkConfigUtils.parseFlatJsonMap(json, field);
    Map<String, String> out = new TreeMap<>();
    for (Map.Entry<String, String> entry : parsed.entrySet()) {
      out.put(
          entry.getKey(),
          MaintenanceAuthConfigs.resolveConfigReference(gravitinoConfig, entry.getValue()));
    }
    return writeJson(out, field);
  }

  private String resolveTasksJson(String tasksJson) {
    try {
      JsonNode root = JsonUtils.anyFieldMapper().readTree(tasksJson);
      Preconditions.checkArgument(root != null && root.isArray(), "tasks must be a JSON array");
      for (JsonNode task : root) {
        if (task == null || !task.isObject()) {
          continue;
        }
        JsonNode paramsNode = task.get(MaintenanceJobTemplates.PARAMS_KEY);
        if (paramsNode == null || !paramsNode.isObject()) {
          continue;
        }
        ObjectNode params = (ObjectNode) paramsNode;
        List<String> keys = new ArrayList<>();
        Iterator<String> names = params.fieldNames();
        while (names.hasNext()) {
          keys.add(names.next());
        }
        for (String paramKey : keys) {
          JsonNode value = params.get(paramKey);
          if (value == null || !value.isTextual()) {
            continue;
          }
          params.put(
              paramKey,
              MaintenanceAuthConfigs.resolveConfigReference(gravitinoConfig, value.asText()));
        }
      }
      return JsonUtils.anyFieldMapper().writeValueAsString(root);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to resolve " + MaintenanceJobTemplates.TASKS_KEY, e);
    }
  }

  /**
   * Routes jobOptions like main: procedure → parameters; other keys → {@code spark_conf}. Resource
   * / catalog placeholders are applied by {@link
   * IcebergSparkConfigUtils#overlayFlatResourceAndCatalogKeys}.
   */
  private static void routeJobOption(
      Map<String, String> parameters,
      Map<String, String> sparkConf,
      String catalogName,
      String key,
      String value) {
    if (PROCEDURE_PARAMETER_KEYS.contains(key)) {
      parameters.put(key, value);
      return;
    }
    if (IcebergSparkConfigUtils.isFlatResourceOrCatalogKey(key)) {
      return;
    }
    if (key.startsWith("spark.") || key.startsWith(SPARK_SQL_CATALOG_PREFIX)) {
      sparkConf.put(key, value);
      return;
    }
    sparkConf.put(SPARK_SQL_CATALOG_PREFIX + catalogName + "." + key, value);
  }

  private Map<String, String> ensureUpdaterCallbackOptions(
      PolicyEntity policy, Map<String, String> overlaid) {
    Map<String, String> updaterOptions = new LinkedHashMap<>();
    if (overlaid != null) {
      updaterOptions.putAll(overlaid);
    }
    updaterOptions.put("metalake", policy.namespace().level(0));
    String gravitinoUri = updaterOptions.get("gravitino_uri");
    if (gravitinoUri == null || gravitinoUri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "updater_options.gravitino_uri is required for Spark submit; set policy "
              + "updaterOptions.gravitino_uri or gravitino.maintenance.gravitinoAuth.uri");
    }
    return updaterOptions;
  }

  private void putTypedProcedureParams(
      Map<String, String> parameters, @Nullable PolicyContent content) {
    if (content instanceof IcebergDataCompactionContent) {
      IcebergDataCompactionContent compaction = (IcebergDataCompactionContent) content;
      parameters.put("strategy", compaction.rewriteStrategy());
      if (compaction.sortOrder() != null && !compaction.sortOrder().isEmpty()) {
        parameters.put("sort_order", compaction.sortOrder());
      }
      if (!compaction.rewriteOptions().isEmpty()) {
        parameters.put("options", writeJson(new TreeMap<>(compaction.rewriteOptions()), "options"));
      }
      return;
    }
    if (content instanceof IcebergRewriteManifestsContent) {
      IcebergRewriteManifestsContent rewrite = (IcebergRewriteManifestsContent) content;
      if (rewrite.specId() != null) {
        parameters.put("spec_id", Integer.toString(rewrite.specId()));
      }
      if (rewrite.useCaching() != null) {
        parameters.put("use_caching", Boolean.toString(rewrite.useCaching()));
      }
      return;
    }
    if (content instanceof IcebergOrphanFileRemovalContent) {
      IcebergOrphanFileRemovalContent orphan = (IcebergOrphanFileRemovalContent) content;
      IcebergOrphanFileRemovalContent.validateOlderThanDays(orphan.olderThanDays());
      parameters.put("older_than", Long.toString(orphan.olderThanDays()));
      parameters.put("dry_run", Boolean.toString(orphan.dryRun()));
      if (orphan.location() != null) {
        parameters.put("location", orphan.location());
      }
      return;
    }
    if (content instanceof IcebergSnapshotExpirationContent) {
      IcebergSnapshotExpirationContent expire = (IcebergSnapshotExpirationContent) content;
      IcebergSnapshotExpirationContent.validateOlderThanDays(expire.olderThanDays());
      IcebergSnapshotExpirationContent.validateRetainLast(expire.retainLast());
      if (expire.olderThanDays() == IcebergSnapshotExpirationContent.DEFAULT_OLDER_THAN_DAYS) {
        parameters.putIfAbsent("older_than", "");
      } else {
        parameters.putIfAbsent("older_than", Long.toString(expire.olderThanDays()));
      }
      if (expire.retainLast() == IcebergSnapshotExpirationContent.DEFAULT_RETAIN_LAST) {
        parameters.putIfAbsent("retain_last", "");
      } else {
        parameters.putIfAbsent("retain_last", Long.toString(expire.retainLast()));
      }
      parameters.putIfAbsent("stream_results", Boolean.toString(expire.streamResults()));
    }
  }

  private static long parsePositiveDays(String value, String field) {
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(field + " must be an integer", e);
    }
  }

  private static PolicyExecutionSpec executionSpec(@Nullable PolicyContent content) {
    if (content instanceof ExecutablePolicyContent) {
      return ((ExecutablePolicyContent) content).executionSpec();
    }
    return PolicyExecutionSpec.empty();
  }

  private static String writeJson(Map<String, ?> map, String field) {
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(map);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize " + field + " for runJob", e);
    }
  }

  /** Schedule-time output of {@link #build}. */
  public static final class ScheduleConf {
    private final Map<String, String> parameters;
    private final String updaterOptions;
    @Nullable private final String sparkConf;

    ScheduleConf(
        Map<String, String> parameters, String updaterOptions, @Nullable String sparkConf) {
      this.parameters = Map.copyOf(Preconditions.checkNotNull(parameters, "parameters"));
      this.updaterOptions = Preconditions.checkNotNull(updaterOptions, "updaterOptions");
      this.sparkConf = sparkConf;
    }

    public Map<String, String> parameters() {
      return parameters;
    }

    public String updaterOptions() {
      return updaterOptions;
    }

    @Nullable
    public String sparkConf() {
      return sparkConf;
    }

    /** Flat map for tests / {@link #resolveForSubmit}. */
    public Map<String, String> asMap() {
      Map<String, String> out = new LinkedHashMap<>(parameters);
      out.put(MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY, updaterOptions);
      if (sparkConf != null && !sparkConf.isBlank()) {
        out.put("spark_conf", sparkConf);
      }
      return out;
    }
  }
}
