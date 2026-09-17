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

package org.apache.gravitino.maintenance.optimizer.command;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.GravitinoClientUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;

/**
 * Handles CLI command {@code submit-update-stats-job} for submitting built-in Iceberg update stats
 * jobs directly from optimizer CLI.
 */
public class SubmitUpdateStatsJobCommand implements OptimizerCommandExecutor {

  private static final String JOB_TEMPLATE_NAME = "builtin-iceberg-update-stats";
  private static final String DEFAULT_UPDATE_MODE = "all";
  private static final String OPTION_UPDATER_OPTIONS = "updater-options";
  private static final String OPTION_SPARK_CONF = "spark-conf";
  private static final String DEFAULT_SPARK_MASTER = "local[*]";
  private static final String DEFAULT_SPARK_EXECUTOR_INSTANCES = "1";
  private static final String DEFAULT_SPARK_EXECUTOR_CORES = "1";
  private static final String DEFAULT_SPARK_EXECUTOR_MEMORY = "1g";
  private static final String DEFAULT_SPARK_DRIVER_MEMORY = "1g";
  private static final String SPARK_MASTER_KEY = "spark.master";
  private static final String SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances";
  private static final String SPARK_EXECUTOR_CORES_KEY = "spark.executor.cores";
  private static final String SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory";
  private static final String SPARK_DRIVER_MEMORY_KEY = "spark.driver.memory";
  private static final String SPARK_SQL_CATALOG_PREFIX = "spark.sql.catalog.";
  private static final String ICEBERG_SPARK_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog";
  private static final String REDACTED = "***";
  private static final Set<String> SENSITIVE_UPDATER_OPTION_KEYS =
      new HashSet<>(
          List.of(
              "password",
              "oauth_credential",
              "oauth_token",
              OptimizerConfig.AUTH_PASSWORD,
              OptimizerConfig.AUTH_OAUTH_CREDENTIAL));

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public void execute(OptimizerCommandContext context) throws Exception {
    Map<String, String> submitterConfigs = context.optimizerEnv().config().jobSubmitterConfigs();

    List<TableTarget> tableTargets =
        parseTableTargets(
            context.identifiers(),
            context.optimizerEnv().config().get(OptimizerConfig.GRAVITINO_DEFAULT_CATALOG_CONFIG));

    String updateMode =
        parseUpdateMode(
            resolveScalarOption(context.updateMode(), submitterConfigs.get("update_mode")));

    String updaterOptionsJson =
        resolveJsonOption(context.updaterOptions(), submitterConfigs.get("updater_options"));
    String sparkConfJson =
        resolveJsonOption(context.sparkConf(), submitterConfigs.get("spark_conf"));

    Map<String, String> updaterOptions =
        IcebergSparkConfigUtils.parseFlatJsonMap(updaterOptionsJson, OPTION_UPDATER_OPTIONS);
    Map<String, String> sparkConfigs =
        normalizeSparkConfigs(
            tableTargets,
            IcebergSparkConfigUtils.parseFlatJsonMap(sparkConfJson, OPTION_SPARK_CONF));

    validateUpdaterOptions(updateMode, updaterOptions);
    validateSparkConfigs(tableTargets, sparkConfigs);

    if (context.dryRun()) {
      for (TableTarget tableTarget : tableTargets) {
        Map<String, String> jobConfig =
            buildJobConfig(tableTarget, updateMode, updaterOptions, sparkConfigs, submitterConfigs);
        context
            .output()
            .printf(
                "DRY-RUN: identifier=%s jobTemplate=%s jobConfig=%s%n",
                tableTarget.fullIdentifier, JOB_TEMPLATE_NAME, redactJobConfigForLog(jobConfig));
      }
      context
          .output()
          .printf("SUMMARY: submit-update-stats-job total=%d dryRun=true%n", tableTargets.size());
      return;
    }

    try (GravitinoClient client =
        GravitinoClientUtils.createClient(context.optimizerEnv(), updaterOptions)) {
      int submitted = 0;
      for (TableTarget tableTarget : tableTargets) {
        Map<String, String> jobConfig =
            buildJobConfig(tableTarget, updateMode, updaterOptions, sparkConfigs, submitterConfigs);
        JobHandle jobHandle = client.runJob(JOB_TEMPLATE_NAME, jobConfig);
        submitted++;
        context
            .output()
            .printf(
                "SUBMIT: identifier=%s jobTemplate=%s jobId=%s jobConfig=%s%n",
                tableTarget.fullIdentifier,
                JOB_TEMPLATE_NAME,
                jobHandle.jobId(),
                redactJobConfigForLog(jobConfig));
      }
      context
          .output()
          .printf(
              "SUMMARY: submit-update-stats-job total=%d submitted=%d dryRun=false%n",
              tableTargets.size(), submitted);
    }
  }

  private static Map<String, String> buildJobConfig(
      TableTarget tableTarget,
      String updateMode,
      Map<String, String> updaterOptions,
      Map<String, String> sparkConfigs,
      Map<String, String> submitterConfigs) {
    Map<String, String> jobConfig = new LinkedHashMap<>();
    jobConfig.put("catalog_name", tableTarget.catalogName);
    jobConfig.putAll(
        resolveTemplateSparkPlaceholders(tableTarget.catalogName, sparkConfigs, submitterConfigs));
    jobConfig.put("table_identifier", tableTarget.schemaAndTable);
    jobConfig.put("update_mode", updateMode);
    jobConfig.put("updater_options", toCanonicalJson(updaterOptions));
    jobConfig.put("spark_conf", toCanonicalJson(sparkConfigs));
    return jobConfig;
  }

  /**
   * Returns a copy of {@code jobConfig} safe for logging. Sensitive fields inside {@code
   * updater_options} and {@code spark_conf} JSON maps are replaced with {@code ***}.
   */
  static Map<String, String> redactJobConfigForLog(Map<String, String> jobConfig) {
    if (jobConfig == null || jobConfig.isEmpty()) {
      return jobConfig;
    }
    Map<String, String> redacted = new LinkedHashMap<>(jobConfig);
    redactJsonMapField(redacted, "updater_options");
    redactJsonMapField(redacted, "spark_conf");
    return redacted;
  }

  private static void redactJsonMapField(Map<String, String> jobConfig, String fieldName) {
    String json = jobConfig.get(fieldName);
    if (StringUtils.isBlank(json)) {
      return;
    }
    try {
      Map<String, String> parsed =
          MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
      Map<String, String> safe = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : parsed.entrySet()) {
        if (isSensitiveConfigKey(entry.getKey())) {
          safe.put(entry.getKey(), REDACTED);
        } else {
          safe.put(entry.getKey(), entry.getValue());
        }
      }
      jobConfig.put(fieldName, toCanonicalJson(safe));
    } catch (Exception ignored) {
      // Keep the original value if JSON cannot be parsed for display.
    }
  }

  private static boolean isSensitiveConfigKey(String key) {
    if (StringUtils.isBlank(key)) {
      return false;
    }
    String normalized = key.trim().toLowerCase(Locale.ROOT);
    return SENSITIVE_UPDATER_OPTION_KEYS.contains(key)
        || SENSITIVE_UPDATER_OPTION_KEYS.contains(normalized)
        || normalized.endsWith("password")
        || normalized.endsWith("credential")
        || normalized.endsWith("secret")
        || normalized.endsWith("token")
        || normalized.contains(".password")
        || normalized.contains(".credential")
        || normalized.contains(".token")
        || normalized.contains(".secret");
  }

  private static String resolveScalarOption(String cliValue, String confValue) {
    if (StringUtils.isNotBlank(cliValue)) {
      return cliValue.trim();
    }
    return StringUtils.isBlank(confValue) ? null : confValue.trim();
  }

  private static String resolveJsonOption(String cliValue, String confValue) {
    if (StringUtils.isNotBlank(cliValue)) {
      return cliValue.trim();
    }
    return StringUtils.isBlank(confValue) ? null : confValue.trim();
  }

  private static String parseUpdateMode(String value) {
    String normalized =
        StringUtils.isBlank(value) ? DEFAULT_UPDATE_MODE : value.trim().toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(
        "stats".equals(normalized) || "metrics".equals(normalized) || "all".equals(normalized),
        "Invalid --update-mode: %s. Supported values are: stats, metrics, all",
        value);
    return normalized;
  }

  private static List<TableTarget> parseTableTargets(String[] identifiers, String defaultCatalog) {
    Preconditions.checkArgument(
        identifiers != null && identifiers.length > 0,
        "Missing required option --identifiers for command 'submit-update-stats-job'");

    List<TableTarget> tableTargets = new ArrayList<>();
    for (String rawIdentifier : identifiers) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(rawIdentifier), "--identifiers contains blank identifier");
      String[] levels = rawIdentifier.trim().split("\\.");
      if (levels.length == 3) {
        tableTargets.add(
            new TableTarget(
                rawIdentifier.trim(),
                requireNonBlank(levels[0], "catalog"),
                requireNonBlank(levels[1], "schema") + "." + requireNonBlank(levels[2], "table")));
      } else if (levels.length == 2) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(defaultCatalog),
            "Identifier '%s' uses schema.table format, but %s is not configured",
            rawIdentifier,
            OptimizerConfig.GRAVITINO_DEFAULT_CATALOG);
        tableTargets.add(
            new TableTarget(
                defaultCatalog + "." + rawIdentifier.trim(),
                defaultCatalog.trim(),
                requireNonBlank(levels[0], "schema") + "." + requireNonBlank(levels[1], "table")));
      } else {
        throw new IllegalArgumentException(
            String.format(
                Locale.ROOT,
                "Identifier '%s' is invalid. Use catalog.schema.table or schema.table",
                rawIdentifier));
      }
    }
    return tableTargets;
  }

  private static String requireNonBlank(String value, String levelName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "%s in identifier cannot be blank", levelName);
    return value.trim();
  }

  private static void validateUpdaterOptions(
      String updateMode, Map<String, String> updaterOptions) {
    if (!"stats".equals(updateMode) && !"all".equals(updateMode)) {
      return;
    }
    String gravitinoUri = StringUtils.trimToNull(updaterOptions.get("gravitino_uri"));
    String metalake = StringUtils.trimToNull(updaterOptions.get("metalake"));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri),
        "Option --updater-options (or config key "
            + OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX
            + "updater_options) "
            + "must contain 'gravitino_uri' when update_mode is stats or all");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        "Option --updater-options (or config key "
            + OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX
            + "updater_options) "
            + "must contain 'metalake' when update_mode is stats or all");
  }

  private static void validateSparkConfigs(
      List<TableTarget> tableTargets, Map<String, String> sparkConfigs) {
    Preconditions.checkArgument(
        !sparkConfigs.isEmpty(),
        "Missing spark config. Set --spark-conf or "
            + "gravitino.optimizer.jobSubmitterConfig.spark_conf in the config file");

    for (TableTarget tableTarget : tableTargets) {
      IcebergSparkConfigUtils.validateSparkConfigsForCatalog(
          sparkConfigs, tableTarget.catalogName, tableTarget.fullIdentifier);
    }
  }

  /**
   * Fill Spark/Iceberg defaults that are already defined in built-in template configs so users do
   * not need to repeat them in --spark-conf.
   */
  private static Map<String, String> normalizeSparkConfigs(
      List<TableTarget> tableTargets, Map<String, String> sparkConfigs) {
    Map<String, String> normalized = new LinkedHashMap<>(sparkConfigs);

    for (TableTarget tableTarget : tableTargets) {
      normalized.putIfAbsent(
          SPARK_SQL_CATALOG_PREFIX + tableTarget.catalogName, ICEBERG_SPARK_CATALOG_IMPL);
    }

    return normalized;
  }

  private static String toCanonicalJson(Map<String, String> options) {
    try {
      return MAPPER.writeValueAsString(new TreeMap<>(options == null ? Map.of() : options));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize options as JSON", e);
    }
  }

  private static Map<String, String> resolveTemplateSparkPlaceholders(
      String catalogName, Map<String, String> sparkConfigs, Map<String, String> submitterConfigs) {
    Map<String, String> placeholders = new LinkedHashMap<>();
    String catalogPrefix = SPARK_SQL_CATALOG_PREFIX + catalogName;

    placeholders.put(
        "spark_master",
        firstNonBlank(
            submitterConfigs.get("spark_master"),
            sparkConfigs.get(SPARK_MASTER_KEY),
            DEFAULT_SPARK_MASTER));
    placeholders.put(
        "spark_executor_instances",
        firstNonBlank(
            submitterConfigs.get("spark_executor_instances"),
            sparkConfigs.get(SPARK_EXECUTOR_INSTANCES_KEY),
            DEFAULT_SPARK_EXECUTOR_INSTANCES));
    placeholders.put(
        "spark_executor_cores",
        firstNonBlank(
            submitterConfigs.get("spark_executor_cores"),
            sparkConfigs.get(SPARK_EXECUTOR_CORES_KEY),
            DEFAULT_SPARK_EXECUTOR_CORES));
    placeholders.put(
        "spark_executor_memory",
        firstNonBlank(
            submitterConfigs.get("spark_executor_memory"),
            sparkConfigs.get(SPARK_EXECUTOR_MEMORY_KEY),
            DEFAULT_SPARK_EXECUTOR_MEMORY));
    placeholders.put(
        "spark_driver_memory",
        firstNonBlank(
            submitterConfigs.get("spark_driver_memory"),
            sparkConfigs.get(SPARK_DRIVER_MEMORY_KEY),
            DEFAULT_SPARK_DRIVER_MEMORY));
    placeholders.put(
        "catalog_type",
        firstNonBlank(
            submitterConfigs.get("catalog_type"), sparkConfigs.get(catalogPrefix + ".type")));
    placeholders.put(
        "catalog_uri",
        firstNonBlank(
            submitterConfigs.get("catalog_uri"), sparkConfigs.get(catalogPrefix + ".uri"), ""));
    placeholders.put(
        "warehouse_location",
        firstNonBlank(
            submitterConfigs.get("warehouse_location"),
            sparkConfigs.get(catalogPrefix + ".warehouse"),
            ""));
    return placeholders;
  }

  private static String firstNonBlank(String... candidates) {
    for (String candidate : candidates) {
      if (StringUtils.isNotBlank(candidate)) {
        return candidate.trim();
      }
    }
    return "";
  }

  private static final class TableTarget {
    private final String fullIdentifier;
    private final String catalogName;
    private final String schemaAndTable;

    private TableTarget(String fullIdentifier, String catalogName, String schemaAndTable) {
      this.fullIdentifier = fullIdentifier;
      this.catalogName = catalogName;
      this.schemaAndTable = schemaAndTable;
    }
  }
}
