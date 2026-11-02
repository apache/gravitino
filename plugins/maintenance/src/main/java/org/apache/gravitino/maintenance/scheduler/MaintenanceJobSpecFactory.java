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
package org.apache.gravitino.maintenance.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Builds enqueue-time {@link MaintenanceJobSpec} values for the cron path and helpers used by cron
 * / commit submit to assemble the flat {@code runJob} map.
 */
public final class MaintenanceJobSpecFactory {

  private static final Pattern RESOURCE_NUMBER = Pattern.compile("^(\\d+)");
  private static final Pattern MEMORY_VALUE =
      Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([kmgt]?i?b?)$", Pattern.CASE_INSENSITIVE);

  /**
   * Keys that belong on the {@code task_data} envelope or are re-derived at submit — not stored in
   * per-task {@code parameters}.
   */
  private static final Set<String> NON_PARAMETER_KEYS =
      ImmutableSet.of(
          MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY,
          "spark_conf",
          MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY,
          MaintenanceJobTemplates.POLICY_IDS_KEY,
          MaintenanceJobTemplates.ACTION_IDS_KEY,
          "tables",
          "table_id",
          "spark_master",
          "spark_executor_instances",
          "spark_executor_cores",
          "spark_executor_memory",
          "spark_driver_memory",
          "catalog_type",
          "catalog_uri",
          "warehouse_location",
          "gravitino_job_id");

  private final MaintenanceJobConfBuilder jobConfBuilder;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  public MaintenanceJobSpecFactory(Config gravitinoConfig) {
    this.jobConfBuilder =
        new MaintenanceJobConfBuilder(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Cron spec for one or more tables under a single policy. Shared {@code updater_options} / {@code
   * spark_conf} sit on the envelope; each table becomes one {@link Task} with its own {@code
   * parameters}.
   *
   * <p>All tables must share the same Spark catalog name so catalog-scoped {@code spark_conf} keys
   * stay correct for every member of a batch.
   *
   * @param policy effective maintenance policy
   * @param tables resolved table refs (same order as enqueue)
   * @return spec ready for {@code task_data}
   */
  public MaintenanceJobSpec forCron(PolicyEntity policy, List<ResolvedTableRef> tables) {
    Preconditions.checkNotNull(policy, "policy");
    Preconditions.checkArgument(tables != null && !tables.isEmpty(), "tables must not be empty");
    Optional<String> wire = MaintenanceJobTemplates.maintenanceType(policy.policyType());
    Preconditions.checkArgument(
        wire.isPresent(), "unsupported policy type %s", policy.policyType());

    String catalogName = tables.get(0).catalogName();
    for (ResolvedTableRef ref : tables) {
      Preconditions.checkArgument(
          catalogName.equals(ref.catalogName()),
          "cron batch tables must share catalogName; got %s and %s",
          catalogName,
          ref.catalogName());
    }

    Map<String, String> built = jobConfBuilder.build(policy, catalogName);
    MaintenanceJobConfBuilder.foldPlaceholdersIntoSparkConf(built, catalogName);
    EnqueueBags bags = splitEnqueueBags(built);

    List<Task> tasks = new ArrayList<>(tables.size());
    for (ResolvedTableRef ref : tables) {
      Map<String, String> parameters = new LinkedHashMap<>(bags.parameters);
      parameters.put("catalog_name", ref.catalogName());
      parameters.put("table_identifier", ref.tableIdentifier());
      tasks.add(new Task(ref.tableId(), policy.id(), wire.get(), null, parameters));
    }
    return new MaintenanceJobSpec(tasks, bags.updaterOptions, bags.sparkConf);
  }

  /**
   * Assembles the flat {@code runJob} map for one cron task: task {@code parameters} + envelope
   * bags + TMS wire keys + one-element {@code tables} JSON.
   *
   * @param spec enqueue spec
   * @param task task to submit
   * @return mutable jobConf (caller resolves secrets / injects {@code action_ids})
   */
  public static Map<String, String> jobConfForTask(MaintenanceJobSpec spec, Task task) {
    Preconditions.checkNotNull(spec, "spec");
    Preconditions.checkNotNull(task, "task");
    String catalogName = task.catalogName();
    String tableIdentifier = task.tableIdentifier();
    Preconditions.checkArgument(
        catalogName != null && tableIdentifier != null,
        "task tableId=%s is missing catalog_name/table_identifier in parameters",
        task.getTableId());

    Map<String, String> jobConf = new LinkedHashMap<>(task.getParameters());
    putSharedBags(jobConf, spec);
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, task.getMaintenanceType());
    jobConf.put(MaintenanceJobTemplates.POLICY_IDS_KEY, Long.toString(task.getPolicyId()));
    jobConf.put("tables", toTablesJson(task.getTableId(), catalogName, tableIdentifier));
    jobConf.remove("table_identifier");
    jobConf.remove("table_id");
    MaintenanceJobConfBuilder.applySparkConfPlaceholders(jobConf, catalogName);
    return jobConf;
  }

  /**
   * Assembles one chained commit {@code runJob} map from all tasks (merged parameters + shared
   * bags).
   *
   * @param spec commit-chain spec
   * @return mutable jobConf
   */
  public static Map<String, String> jobConfForCommitChain(MaintenanceJobSpec spec) {
    Preconditions.checkNotNull(spec, "spec");
    Preconditions.checkArgument(
        spec.getTasks() != null && !spec.getTasks().isEmpty(), "tasks must not be empty");
    Task first = spec.getTasks().get(0);
    String catalogName = first.catalogName();
    String tableIdentifier = first.tableIdentifier();
    Preconditions.checkArgument(
        catalogName != null && tableIdentifier != null,
        "commit task tableId=%s is missing catalog_name/table_identifier",
        first.getTableId());

    Map<String, String> jobConf = new LinkedHashMap<>(first.getParameters());
    for (int i = 1; i < spec.getTasks().size(); i++) {
      Map<String, String> later = spec.getTasks().get(i).getParameters();
      mergeTaskParameters(jobConf, later);
      mergePolicyRulesByType(jobConf, later);
    }
    putSharedBags(jobConf, spec);

    List<String> types = new ArrayList<>(spec.getTasks().size());
    List<String> policyIds = new ArrayList<>(spec.getTasks().size());
    for (Task task : spec.getTasks()) {
      types.add(task.getMaintenanceType());
      policyIds.add(Long.toString(task.getPolicyId()));
    }
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, String.join(",", types));
    jobConf.put(MaintenanceJobTemplates.POLICY_IDS_KEY, String.join(",", policyIds));
    jobConf.put("tables", toTablesJson(first.getTableId(), catalogName, tableIdentifier));
    jobConf.remove("table_identifier");
    jobConf.remove("table_id");
    MaintenanceJobConfBuilder.applySparkConfPlaceholders(jobConf, catalogName);
    return jobConf;
  }

  private static void putSharedBags(Map<String, String> jobConf, MaintenanceJobSpec spec) {
    if (spec.getUpdaterOptions() != null && !spec.getUpdaterOptions().isBlank()) {
      jobConf.put(MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY, spec.getUpdaterOptions());
    }
    if (spec.getSparkConf() != null && !spec.getSparkConf().isBlank()) {
      jobConf.put("spark_conf", spec.getSparkConf());
    }
  }

  /**
   * Splits a {@link MaintenanceJobConfBuilder#build} map into envelope bags vs per-task parameters.
   */
  public static EnqueueBags splitEnqueueBags(Map<String, String> built) {
    Preconditions.checkNotNull(built, "built");
    Map<String, String> parameters = new LinkedHashMap<>();
    String updaterOptions = null;
    String sparkConf = null;
    for (Map.Entry<String, String> entry : built.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY.equals(key)) {
        updaterOptions = value;
        continue;
      }
      if ("spark_conf".equals(key)) {
        sparkConf = value;
        continue;
      }
      if (NON_PARAMETER_KEYS.contains(key)) {
        continue;
      }
      parameters.put(key, value);
    }
    Preconditions.checkArgument(
        updaterOptions != null && !updaterOptions.isBlank(),
        "updater_options is required on enqueue jobConf");
    return new EnqueueBags(parameters, updaterOptions, sparkConf);
  }

  /**
   * Max-merge resource-shaped keys for commit-chain spark_conf / leftover flat keys.
   *
   * @param into baseline map (mutated)
   * @param from candidate map
   */
  public static void mergeResourceKeys(Map<String, String> into, Map<String, String> from) {
    for (Map.Entry<String, String> entry : from.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null || !isResourceKey(key)) {
        continue;
      }
      String existing = into.get(key);
      if (existing == null) {
        into.put(key, value);
        continue;
      }
      Optional<Long> existingNum = resourceMagnitude(key, existing);
      Optional<Long> candidateNum = resourceMagnitude(key, value);
      if (existingNum.isPresent()
          && candidateNum.isPresent()
          && candidateNum.get() > existingNum.get()) {
        into.put(key, value);
      }
    }
  }

  /**
   * Merges later-task procedure parameters into the first task's map (first-wins except resource
   * keys; {@code rules} merged separately).
   */
  static void mergeTaskParameters(Map<String, String> into, Map<String, String> from) {
    mergeResourceKeys(into, from);
    for (Map.Entry<String, String> entry : from.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null
          || value == null
          || MaintenanceJobTemplates.RULES_KEY.equals(key)
          || "catalog_name".equals(key)
          || "table_identifier".equals(key)) {
        continue;
      }
      into.putIfAbsent(key, value);
    }
  }

  /**
   * Union {@link MaintenanceJobTemplates#RULES_KEY} maps so each chain type keeps its enqueue-time
   * rules.
   */
  static void mergePolicyRulesByType(Map<String, String> into, Map<String, String> from) {
    String fromJson = from.get(MaintenanceJobTemplates.RULES_KEY);
    if (fromJson == null || fromJson.trim().isEmpty()) {
      return;
    }
    Map<String, Map<String, Object>> merged =
        new LinkedHashMap<>(parseRulesByType(into.get(MaintenanceJobTemplates.RULES_KEY)));
    for (Map.Entry<String, Map<String, Object>> entry : parseRulesByType(fromJson).entrySet()) {
      merged.putIfAbsent(entry.getKey(), entry.getValue());
    }
    try {
      into.put(
          MaintenanceJobTemplates.RULES_KEY, JsonUtils.anyFieldMapper().writeValueAsString(merged));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(
          "Failed to serialize " + MaintenanceJobTemplates.RULES_KEY, e);
    }
  }

  private static Map<String, Map<String, Object>> parseRulesByType(@Nullable String json) {
    if (json == null || json.trim().isEmpty()) {
      return Map.of();
    }
    try {
      Map<String, Map<String, Object>> parsed =
          JsonUtils.anyFieldMapper()
              .readValue(json, new TypeReference<Map<String, Map<String, Object>>>() {});
      return parsed == null ? Map.of() : parsed;
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to parse " + MaintenanceJobTemplates.RULES_KEY, e);
    }
  }

  private static String toTablesJson(long tableId, String catalogName, String tableIdentifier) {
    List<Map<String, Object>> payload = new ArrayList<>(1);
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("table_id", tableId);
    entry.put("catalog_name", catalogName);
    entry.put("table_identifier", tableIdentifier);
    payload.add(entry);
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize tables for runJob", e);
    }
  }

  private static boolean isResourceKey(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("memory") || lower.contains("cores") || lower.contains("instances");
  }

  private static Optional<Long> resourceMagnitude(String key, String value) {
    if (key.toLowerCase(Locale.ROOT).contains("memory")) {
      return parseMemoryBytes(value);
    }
    return leadingLong(value);
  }

  /**
   * Parses Spark-style memory strings ({@code 512m}, {@code 1g}, {@code 2048}) into bytes for max
   * merge.
   */
  @VisibleForTesting
  static Optional<Long> parseMemoryBytes(String value) {
    if (value == null) {
      return Optional.empty();
    }
    Matcher matcher = MEMORY_VALUE.matcher(value.trim());
    if (!matcher.matches()) {
      return leadingLong(value);
    }
    try {
      double amount = Double.parseDouble(matcher.group(1));
      String unit = matcher.group(2) == null ? "" : matcher.group(2).toLowerCase(Locale.ROOT);
      long multiplier;
      switch (unit) {
        case "":
        case "b":
          multiplier = 1L;
          break;
        case "k":
        case "kb":
        case "ki":
        case "kib":
          multiplier = 1024L;
          break;
        case "m":
        case "mb":
        case "mi":
        case "mib":
          multiplier = 1024L * 1024L;
          break;
        case "g":
        case "gb":
        case "gi":
        case "gib":
          multiplier = 1024L * 1024L * 1024L;
          break;
        case "t":
        case "tb":
        case "ti":
        case "tib":
          multiplier = 1024L * 1024L * 1024L * 1024L;
          break;
        default:
          return Optional.empty();
      }
      return Optional.of(Math.round(amount * multiplier));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private static Optional<Long> leadingLong(String value) {
    Matcher matcher = RESOURCE_NUMBER.matcher(value.trim());
    if (!matcher.find()) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(matcher.group(1)));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /** Visible for tests that assert resource merge. */
  @VisibleForTesting
  public static Map<String, String> newLinkedMap(String... kv) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i + 1 < kv.length; i += 2) {
      map.put(kv[i], kv[i + 1]);
    }
    return map;
  }

  /** Envelope bags peeled from {@link MaintenanceJobConfBuilder#build}. */
  public static final class EnqueueBags {
    /** Per-task template placeholders (no updater/spark session bags). */
    public final Map<String, String> parameters;
    /** Shared updater_options JSON string. */
    public final String updaterOptions;
    /** Shared spark_conf JSON string; may be null. */
    @Nullable public final String sparkConf;

    EnqueueBags(Map<String, String> parameters, String updaterOptions, @Nullable String sparkConf) {
      this.parameters = parameters;
      this.updaterOptions = updaterOptions;
      this.sparkConf = sparkConf;
    }
  }
}
