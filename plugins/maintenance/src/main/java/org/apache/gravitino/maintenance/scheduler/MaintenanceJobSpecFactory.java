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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Operation;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.TableTarget;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Builds enqueue-time {@link MaintenanceJobSpec} values for the cron path and shared jobConf
 * helpers used by cron pick / commit merge.
 */
public final class MaintenanceJobSpecFactory {

  private static final Pattern RESOURCE_NUMBER = Pattern.compile("^(\\d+)");
  private static final Pattern MEMORY_VALUE =
      Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*([kmgt]?i?b?)$", Pattern.CASE_INSENSITIVE);

  private final MaintenanceJobConfBuilder jobConfBuilder;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  public MaintenanceJobSpecFactory(Config gravitinoConfig) {
    this.jobConfBuilder =
        new MaintenanceJobConfBuilder(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Cron spec for one or more tables under a single policy. Shared {@code jobConf} omits per-table
   * keys; pick overlays {@code catalog_name} / {@code table_identifier} for each {@link
   * TableTarget}.
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

    String metalake = policy.namespace().level(0);
    Map<String, String> jobConf = jobConfBuilder.build(policy, catalogName);
    // Per-table keys are applied at pick so one batch spec can cover many tables.
    jobConf.remove("catalog_name");
    jobConf.remove("table_identifier");

    List<TableTarget> targets = new ArrayList<>(tables.size());
    for (ResolvedTableRef ref : tables) {
      targets.add(toTarget(metalake, ref));
    }
    List<Operation> operations = List.of(new Operation(policy.id(), wire.get()));
    return new MaintenanceJobSpec(targets, operations, jobConf);
  }

  /**
   * Copies {@code shared} and overlays catalog plus a one-element {@code tables} JSON for submit.
   *
   * <p>Entity {@code table_id} stays inside {@code tables} (system-filled). It is not a top-level
   * template placeholder — users submitting the built-in template do not know entity ids.
   *
   * @param shared jobConf
   * @param target table to submit
   * @return mutable per-table jobConf
   */
  public static Map<String, String> jobConfForTable(
      Map<String, String> shared, TableTarget target) {
    Preconditions.checkNotNull(shared, "shared");
    Preconditions.checkNotNull(target, "target");
    String catalogName = target.catalogName();
    String tableIdentifier = target.sparkTableIdentifier();
    Preconditions.checkArgument(
        catalogName != null && tableIdentifier != null,
        "table target %s is missing catalog/table identifier levels",
        target.getTableId());
    Map<String, String> jobConf = new LinkedHashMap<>(shared);
    jobConf.put("catalog_name", catalogName);
    jobConf.put("tables", toTablesJson(List.of(target)));
    jobConf.remove("table_identifier");
    jobConf.remove("table_id");
    return jobConf;
  }

  private static String toTablesJson(List<TableTarget> tables) {
    List<Map<String, Object>> payload = new ArrayList<>(tables.size());
    for (TableTarget target : tables) {
      String catalogName = target.catalogName();
      String tableIdentifier = target.sparkTableIdentifier();
      Preconditions.checkArgument(
          catalogName != null && tableIdentifier != null,
          "table target %s is missing catalog/table identifier levels",
          target.getTableId());
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("table_id", target.getTableId());
      entry.put("catalog_name", catalogName);
      entry.put("table_identifier", tableIdentifier);
      payload.add(entry);
    }
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(payload);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize tables for runJob", e);
    }
  }

  /**
   * For resource-shaped keys ({@code *memory*}, {@code *cores*}, {@code *instances*}), keep the
   * larger value. Memory keys compare normalized byte sizes ({@code 1g} &gt; {@code 512m}); cores /
   * instances compare the leading integer. Other keys from the first policy are left unchanged.
   *
   * @param into baseline jobConf (mutated)
   * @param from candidate jobConf
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
   * @param metalake metalake name
   * @param ref resolved Spark table ref
   * @return table target with NameIdentifier levels
   */
  public static TableTarget toTarget(String metalake, ResolvedTableRef ref) {
    List<String> levels = new ArrayList<>();
    levels.add(metalake);
    levels.add(ref.catalogName());
    levels.addAll(Arrays.asList(ref.tableIdentifier().split("\\.")));
    return new TableTarget(ref.tableId(), levels);
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
}
