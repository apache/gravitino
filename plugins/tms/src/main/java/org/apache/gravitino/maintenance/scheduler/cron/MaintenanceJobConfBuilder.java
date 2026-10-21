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
package org.apache.gravitino.maintenance.scheduler.cron;

import com.fasterxml.jackson.core.JsonProcessingException;
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
 * Builds {@code runJob} configuration for TMS cron Spark submits.
 *
 * <p>Maps typed compaction / orphan policy fields onto procedure placeholders; expire and manifest
 * rewrite remain jobOptions passthrough until those content types grow real thresholds. Keeps
 * catalog / Spark resource keys as top-level jobConf, and packs Iceberg REST / {@code
 * gravitino.maintenance.ircAuth.*} passthrough properties into {@code spark_conf} under {@code
 * spark.sql.catalog.<catalogName>.} so SparkSession can authenticate to IRC.
 */
final class MaintenanceJobConfBuilder {

  private static final DateTimeFormatter ORPHAN_CUTOFF =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ssXXX").withZone(ZoneOffset.UTC);

  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";

  /**
   * JobConf keys consumed by the TMS / Iceberg Spark templates as top-level placeholders (not
   * catalog properties).
   */
  private static final Set<String> TEMPLATE_JOB_CONF_KEYS =
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
          "spark_conf",
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
          MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY,
          MaintenanceSparkSubmitPipeline.UPDATER_OPTIONS_JOB_CONF_KEY,
          MaintenanceSparkSubmitPipeline.TMS_TABLE_ID_KEY,
          MaintenanceSparkSubmitPipeline.TMS_POLICY_ID_KEY,
          "gravitino_job_id");

  private final Config gravitinoConfig;
  private final Clock clock;

  MaintenanceJobConfBuilder(Config gravitinoConfig) {
    this(gravitinoConfig, Clock.systemUTC());
  }

  MaintenanceJobConfBuilder(Config gravitinoConfig, Clock clock) {
    this.gravitinoConfig = gravitinoConfig;
    this.clock = clock;
  }

  /**
   * @param policy effective maintenance policy
   * @param catalogName Spark catalog alias for IRC property scoping
   * @return mutable jobConf ready for table / policy id overlays
   */
  Map<String, String> build(PolicyEntity policy, String catalogName) {
    PolicyContent content = policy.content();
    PolicyExecutionSpec spec = executionSpec(content);
    Map<String, String> jobOptions =
        MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, spec.jobOptions());
    Map<String, String> updaterOptions =
        ensureUpdaterCallbackOptions(
            policy,
            MaintenanceAuthConfigs.overlayUpdaterOptions(gravitinoConfig, spec.updaterOptions()));

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
      if (key == null || value == null) {
        continue;
      }
      if ("spark_conf".equals(key)) {
        continue;
      }
      if (TEMPLATE_JOB_CONF_KEYS.contains(key)) {
        jobConf.put(key, value);
      } else {
        sparkConf.put(SPARK_SQL_CATALOG_PREFIX + catalogName + "." + key, value);
      }
    }

    putTypedProcedureParams(jobConf, content);

    jobConf.put(MaintenanceSparkSubmitPipeline.TMS_POLICY_ID_KEY, Long.toString(policy.id()));
    MaintenanceJobTemplates.maintenanceType(policy.policyType())
        .ifPresent(type -> jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, type));

    jobConf.put(
        MaintenanceSparkSubmitPipeline.UPDATER_OPTIONS_JOB_CONF_KEY,
        writeJson(updaterOptions, "updater_options"));
    if (!sparkConf.isEmpty()) {
      jobConf.put("spark_conf", writeJson(new TreeMap<>(sparkConf), "spark_conf"));
    }
    return jobConf;
  }

  /**
   * Ensures Spark → Gravitino sample/finish callbacks have {@code metalake} (from the policy
   * namespace) and {@code gravitino_uri} (cluster config, overridable by policy updaterOptions).
   */
  private Map<String, String> ensureUpdaterCallbackOptions(
      PolicyEntity policy, Map<String, String> overlaid) {
    Map<String, String> updaterOptions = new LinkedHashMap<>();
    if (overlaid != null) {
      updaterOptions.putAll(overlaid);
    }
    // Callback metalake must match the policy namespace (authz path + job scope). Policy
    // updaterOptions must not redirect sample/finish to another metalake.
    String metalake = policy.namespace().level(0);
    updaterOptions.put("metalake", metalake);

    Optional<String> configuredUri =
        gravitinoConfig.get(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_URI);
    if (configuredUri.isPresent()) {
      String uri = configuredUri.get().trim();
      if (!uri.isEmpty()) {
        updaterOptions.putIfAbsent("gravitino_uri", uri);
      }
    }

    String gravitinoUri = updaterOptions.get("gravitino_uri");
    if (gravitinoUri == null || gravitinoUri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "updater_options.gravitino_uri is required for TMS cron Spark submit; set policy "
              + "updaterOptions.gravitino_uri or gravitino.maintenance.gravitinoUri");
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
