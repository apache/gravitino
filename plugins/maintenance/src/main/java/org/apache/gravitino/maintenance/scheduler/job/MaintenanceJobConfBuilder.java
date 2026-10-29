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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Builds the {@code runJob} configuration map for Spark submits.
 *
 * <p>Shape matches Iceberg update-stats submit: flatten template placeholders into the top-level
 * map, pack {@code updater_options} as one JSON string, and pack extra Spark / IRC settings into
 * {@code spark_conf} as another JSON string. Policy {@code jobOptions} (including overlay from
 * {@code gravitino.maintenance.ircAuth.*}) keep short IRC keys such as {@code rest.auth.type};
 * those are expanded to {@code spark.sql.catalog.<catalog>.*} only when writing {@code spark_conf}.
 * Dot-form Spark resource keys ({@code spark.executor.memory}) become the matching underscore
 * placeholders ({@code spark_executor_memory}) instead of catalog properties.
 *
 * <p>{@link #build} is for enqueue {@code task_data}: cluster {@code gravitinoAuth} / {@code
 * ircAuth} are stored as conf <em>keys</em> (not secret values). Call {@link #resolveForSubmit}
 * immediately before {@code runJob} to expand those keys from the live {@code gravitino.conf}.
 */
public final class MaintenanceJobConfBuilder {

  private static final DateTimeFormatter ORPHAN_CUTOFF =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ssXXX").withZone(ZoneOffset.UTC);

  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  /** Top-level {@code runJob} placeholders consumed by Iceberg / Spark templates. */
  private static final Set<String> FLAT_PLACEHOLDER_KEYS =
      ImmutableSet.of(
          "catalog_name",
          "table_identifier",
          "catalog_type",
          "catalog_uri",
          "warehouse_location",
          "spark_master",
          "spark_executor_instances",
          "spark_executor_cores",
          "spark_executor_memory",
          "spark_driver_memory",
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
          "dry_run",
          MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY,
          MaintenanceJobTemplates.RULES_KEY,
          "gravitino_job_id");

  /**
   * Spark resource / session keys that map onto flat template placeholders (same as update-stats).
   */
  private static final Map<String, String> SPARK_KEY_TO_PLACEHOLDER =
      ImmutableMap.of(
          "spark.master",
          "spark_master",
          "spark.executor.instances",
          "spark_executor_instances",
          "spark.executor.cores",
          "spark_executor_cores",
          "spark.executor.memory",
          "spark_executor_memory",
          "spark.driver.memory",
          "spark_driver_memory");

  private final Config gravitinoConfig;
  private final Clock clock;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  public MaintenanceJobConfBuilder(Config gravitinoConfig) {
    this(gravitinoConfig, Clock.systemUTC());
  }

  MaintenanceJobConfBuilder(Config gravitinoConfig, Clock clock) {
    this.gravitinoConfig = gravitinoConfig;
    this.clock = clock;
  }

  /**
   * Enqueue-time jobConf for db-scheduler {@code task_data}. Cluster auth overlays store conf keys
   * (for example {@code gravitino.maintenance.gravitinoAuth.uri}), not secret values.
   *
   * @param policy effective maintenance policy
   * @param catalogName Spark catalog alias for IRC property scoping
   * @return mutable jobConf (split into envelope bags / task {@code parameters} by the factory)
   */
  public Map<String, String> build(PolicyEntity policy, String catalogName) {
    PolicyContent content = policy.content();
    PolicyExecutionSpec spec = executionSpec(content);
    // ircAuth suffixes stay short-named; values are conf keys until resolveForSubmit.
    Map<String, String> jobOptions =
        MaintenanceAuthConfigs.overlayJobOptions(
            spec.jobOptions(), MaintenanceAuthConfigs.loadIrcAuthAsConfigKeys(gravitinoConfig));
    Map<String, String> updaterOptions =
        ensureUpdaterCallbackOptions(
            policy,
            MaintenanceAuthConfigs.overlayUpdaterOptions(
                spec.updaterOptions(),
                MaintenanceAuthConfigs.loadGravitinoAuthAsConfigKeys(gravitinoConfig)));

    Map<String, String> jobConf = new LinkedHashMap<>();
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
      routeJobOption(jobConf, sparkConf, catalogName, key, value);
    }

    fillPlaceholdersFromSparkConf(jobConf, sparkConf, catalogName);
    putTypedProcedureParams(jobConf, content);

    Optional<String> maintenanceType = MaintenanceJobTemplates.maintenanceType(policy.policyType());
    maintenanceType.ifPresent(
        type -> {
          jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, type);
          jobConf.put(MaintenanceJobTemplates.POLICY_IDS_KEY, Long.toString(policy.id()));
        });
    if (content != null && maintenanceType.isPresent()) {
      Map<String, Map<String, Object>> rulesByType = new LinkedHashMap<>();
      rulesByType.put(maintenanceType.get(), new LinkedHashMap<>(content.rules()));
      jobConf.put(
          MaintenanceJobTemplates.RULES_KEY,
          writeJson(rulesByType, MaintenanceJobTemplates.RULES_KEY));
    }

    jobConf.put(
        MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY,
        writeJson(updaterOptions, "updater_options"));
    if (!sparkConf.isEmpty()) {
      jobConf.put("spark_conf", writeJson(new TreeMap<>(sparkConf), "spark_conf"));
    }
    return jobConf;
  }

  /**
   * Re-derives flat template placeholders ({@code spark_executor_memory}, {@code catalog_type}, …)
   * from nested {@code spark_conf}. Used when enqueue {@code task_data} stores only the JSON bag.
   *
   * @param jobConf mutable runJob map that already contains {@code spark_conf}
   * @param catalogName Spark catalog alias for IRC property scoping
   */
  public static void applySparkConfPlaceholders(Map<String, String> jobConf, String catalogName) {
    Preconditions.checkNotNull(jobConf, "jobConf");
    Preconditions.checkNotNull(catalogName, "catalogName");
    String sparkConfJson = jobConf.get("spark_conf");
    if (sparkConfJson == null || sparkConfJson.trim().isEmpty()) {
      return;
    }
    Map<String, String> sparkConf =
        IcebergSparkConfigUtils.parseFlatJsonMap(sparkConfJson, "spark_conf");
    fillPlaceholdersFromSparkConf(jobConf, sparkConf, catalogName);
  }

  /**
   * Folds flat session placeholders ({@code spark_executor_memory}, {@code catalog_type}, …) into
   * nested {@code spark_conf} so enqueue {@code task_data} can keep only the JSON bag at the top
   * level.
   *
   * @param jobConf mutable map from {@link #build}
   * @param catalogName Spark catalog alias for IRC property scoping
   */
  public static void foldPlaceholdersIntoSparkConf(
      Map<String, String> jobConf, String catalogName) {
    Preconditions.checkNotNull(jobConf, "jobConf");
    Preconditions.checkNotNull(catalogName, "catalogName");
    Map<String, String> sparkConf =
        new TreeMap<>(
            IcebergSparkConfigUtils.parseFlatJsonMap(jobConf.get("spark_conf"), "spark_conf"));
    for (Map.Entry<String, String> entry : SPARK_KEY_TO_PLACEHOLDER.entrySet()) {
      String sparkKey = entry.getKey();
      String placeholder = entry.getValue();
      String value = jobConf.get(placeholder);
      if (value != null && !value.isBlank()) {
        sparkConf.putIfAbsent(sparkKey, value);
      }
    }
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;
    putIfAbsentSparkFromPlaceholder(jobConf, sparkConf, "catalog_type", catalogPrefix + ".type");
    putIfAbsentSparkFromPlaceholder(jobConf, sparkConf, "catalog_uri", catalogPrefix + ".uri");
    putIfAbsentSparkFromPlaceholder(
        jobConf, sparkConf, "warehouse_location", catalogPrefix + ".warehouse");
    if (!sparkConf.isEmpty()) {
      jobConf.put("spark_conf", writeJson(sparkConf, "spark_conf"));
    }
  }

  private static void putIfAbsentSparkFromPlaceholder(
      Map<String, String> jobConf,
      Map<String, String> sparkConf,
      String placeholder,
      String sparkKey) {
    String value = jobConf.get(placeholder);
    if (value != null && !value.isBlank()) {
      sparkConf.putIfAbsent(sparkKey, value);
    }
  }

  /**
   * Expands {@code gravitino.maintenance.*} conf-key references in an enqueue snapshot into live
   * values from {@code gravitino.conf}. Call immediately before {@code runJob}.
   *
   * @param enqueueJobConf jobConf from {@link #build} / {@code task_data}
   * @return new map with secrets and URIs resolved
   */
  public Map<String, String> resolveForSubmit(Map<String, String> enqueueJobConf) {
    Map<String, String> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : enqueueJobConf.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY.equals(key)
          || "spark_conf".equals(key)) {
        resolved.put(key, resolveJsonMapValues(value, key));
      } else {
        resolved.put(key, MaintenanceAuthConfigs.resolveConfigReference(gravitinoConfig, value));
      }
    }
    return resolved;
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

  private static void routeJobOption(
      Map<String, String> jobConf,
      Map<String, String> sparkConf,
      String catalogName,
      String key,
      String value) {
    if (FLAT_PLACEHOLDER_KEYS.contains(key)) {
      jobConf.put(key, value);
      return;
    }
    String placeholder = SPARK_KEY_TO_PLACEHOLDER.get(key);
    if (placeholder != null) {
      jobConf.put(placeholder, value);
      sparkConf.put(key, value);
      return;
    }
    if (key.startsWith("spark.")) {
      // Spark session / SQL settings stay as full keys in spark_conf (never catalog-prefixed).
      sparkConf.put(key, value);
      return;
    }
    if (key.startsWith(SPARK_SQL_CATALOG_PREFIX)) {
      sparkConf.put(key, value);
      return;
    }
    // IRC / Iceberg catalog props from jobOptions / ircAuth (short names).
    sparkConf.put(SPARK_SQL_CATALOG_PREFIX + catalogName + "." + key, value);
  }

  private static void fillPlaceholdersFromSparkConf(
      Map<String, String> jobConf, Map<String, String> sparkConf, String catalogName) {
    for (Map.Entry<String, String> entry : SPARK_KEY_TO_PLACEHOLDER.entrySet()) {
      String sparkKey = entry.getKey();
      String placeholder = entry.getValue();
      if (!jobConf.containsKey(placeholder) && sparkConf.containsKey(sparkKey)) {
        jobConf.put(placeholder, sparkConf.get(sparkKey));
      }
    }
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;
    putIfAbsentFromSpark(jobConf, sparkConf, "catalog_type", catalogPrefix + ".type");
    putIfAbsentFromSpark(jobConf, sparkConf, "catalog_uri", catalogPrefix + ".uri");
    putIfAbsentFromSpark(jobConf, sparkConf, "warehouse_location", catalogPrefix + ".warehouse");
  }

  private static void putIfAbsentFromSpark(
      Map<String, String> jobConf,
      Map<String, String> sparkConf,
      String placeholder,
      String sparkKey) {
    if (!jobConf.containsKey(placeholder) && sparkConf.containsKey(sparkKey)) {
      jobConf.put(placeholder, sparkConf.get(sparkKey));
    }
  }

  /**
   * Ensures Spark → Gravitino sample/finish callbacks have {@code metalake} (from the policy
   * namespace) and {@code gravitino_uri} (from {@code gravitino.maintenance.gravitinoAuth.uri} via
   * auth overlay, overridable by policy updaterOptions).
   */
  private Map<String, String> ensureUpdaterCallbackOptions(
      PolicyEntity policy, Map<String, String> overlaid) {
    Map<String, String> updaterOptions = new LinkedHashMap<>();
    if (overlaid != null) {
      updaterOptions.putAll(overlaid);
    }
    String metalake = policy.namespace().level(0);
    updaterOptions.put("metalake", metalake);

    String gravitinoUri = updaterOptions.get("gravitino_uri");
    if (gravitinoUri == null || gravitinoUri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "updater_options.gravitino_uri is required for Spark submit; set policy "
              + "updaterOptions.gravitino_uri or gravitino.maintenance.gravitinoAuth.uri");
    }
    return updaterOptions;
  }

  private void putTypedProcedureParams(
      Map<String, String> jobConf, @Nullable PolicyContent content) {
    if (content instanceof IcebergDataCompactionContent) {
      IcebergDataCompactionContent compaction = (IcebergDataCompactionContent) content;
      jobConf.put("strategy", compaction.rewriteStrategy());
      if (compaction.sortOrder() != null && !compaction.sortOrder().isEmpty()) {
        jobConf.put("sort_order", compaction.sortOrder());
      }
      if (!compaction.rewriteOptions().isEmpty()) {
        jobConf.put("options", writeJson(new TreeMap<>(compaction.rewriteOptions()), "options"));
      }
      return;
    }
    if (content instanceof IcebergOrphanFileRemovalContent) {
      IcebergOrphanFileRemovalContent orphan = (IcebergOrphanFileRemovalContent) content;
      IcebergOrphanFileRemovalContent.validateOlderThanDays(orphan.olderThanDays());
      jobConf.put("older_than", orphanCutoff(orphan.olderThanDays()));
      jobConf.put("dry_run", Boolean.toString(orphan.dryRun()));
      if (orphan.location() != null) {
        jobConf.put("location", orphan.location());
      }
    }
  }

  private String orphanCutoff(long days) {
    try {
      return ORPHAN_CUTOFF.format(clock.instant().minus(days, ChronoUnit.DAYS));
    } catch (DateTimeException | ArithmeticException e) {
      throw new IllegalArgumentException(
          "olderThanDays cannot be represented as a cleanup timestamp", e);
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
}
