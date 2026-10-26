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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Operation;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;

/**
 * Builds enqueue-time {@link MaintenanceJobSpec} values for the commit-job path (keeps commit →
 * scheduler dependency one-way; no scheduler → commit package cycle).
 *
 * <p>Chain {@code jobConf} starts from the first policy, takes the max of resource-shaped keys from
 * later members, and {@code putIfAbsent}s remaining keys so later types keep their procedure params
 * (e.g. expire {@code older_than}) when auth bags already agree. {@code rules} is unioned across
 * members so each type keeps its enqueue-time rules for in-Spark recommend.
 */
final class CommitJobSpecFactory {

  private static final Set<String> CHAIN_OWNED_KEYS =
      Set.of(
          MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY,
          MaintenanceJobTemplates.POLICY_IDS_KEY,
          "catalog_name",
          "table_identifier",
          "tables",
          MaintenanceJobTemplates.RULES_KEY);

  private final MaintenanceJobConfBuilder jobConfBuilder;

  /**
   * @param gravitinoConfig server config for auth overlay and gravitino URI
   */
  CommitJobSpecFactory(Config gravitinoConfig) {
    this.jobConfBuilder =
        new MaintenanceJobConfBuilder(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Commit-chain spec for one table. Caller must already have filtered cooldown and verified auth
   * consistency.
   *
   * @param chain non-empty onCommit members in order
   * @param ref resolved Spark table ref
   * @return spec with merged {@code jobConf} including {@code maintenance_types} and {@code
   *     policy_ids}
   */
  MaintenanceJobSpec forCommit(List<CommitChainMember> chain, ResolvedTableRef ref) {
    Preconditions.checkArgument(chain != null && !chain.isEmpty(), "chain must not be empty");
    Preconditions.checkNotNull(ref, "ref");

    CommitChainMember first = chain.get(0);
    String metalake = first.policy().namespace().level(0);
    Map<String, String> jobConf = buildChainJobConf(chain, ref);
    List<TableTarget> targets = List.of(MaintenanceJobSpecFactory.toTarget(metalake, ref));
    List<Operation> operations =
        chain.stream()
            .map(m -> new Operation(m.policy().id(), m.maintenanceType()))
            .collect(Collectors.toList());
    return new MaintenanceJobSpec(targets, operations, jobConf);
  }

  private Map<String, String> buildChainJobConf(
      List<CommitChainMember> chain, ResolvedTableRef ref) {
    CommitChainMember first = chain.get(0);
    Map<String, String> jobConf = jobConfBuilder.build(first.policy(), ref.catalogName());
    for (int i = 1; i < chain.size(); i++) {
      Map<String, String> other = jobConfBuilder.build(chain.get(i).policy(), ref.catalogName());
      mergeLaterMemberJobConf(jobConf, other);
    }
    List<String> types =
        chain.stream().map(CommitChainMember::maintenanceType).collect(Collectors.toList());
    List<String> policyIds =
        chain.stream().map(m -> Long.toString(m.policy().id())).collect(Collectors.toList());
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, String.join(",", types));
    jobConf.put(MaintenanceJobTemplates.POLICY_IDS_KEY, String.join(",", policyIds));
    // Per-table keys (catalog_name / tables JSON) are applied at pick via jobConfForTable.
    jobConf.remove("catalog_name");
    jobConf.remove("table_identifier");
    jobConf.remove("tables");
    return jobConf;
  }

  /**
   * Max-merge resource keys; fill missing non-owned keys from a later chain member (procedure
   * params). First-policy auth / {@code updater_options} win. Nested {@code spark_conf} is merged
   * by key with first-wins {@code putIfAbsent}. {@code rules} are unioned by maintenance type.
   */
  static void mergeLaterMemberJobConf(Map<String, String> into, Map<String, String> from) {
    MaintenanceJobSpecFactory.mergeResourceKeys(into, from);
    mergePolicyRulesByType(into, from);
    mergeSparkConfByKey(into, from);
    for (Map.Entry<String, String> entry : from.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null
          || value == null
          || CHAIN_OWNED_KEYS.contains(key)
          || "spark_conf".equals(key)) {
        continue;
      }
      into.putIfAbsent(key, value);
    }
  }

  /**
   * Merges nested {@code spark_conf} JSON maps. Existing keys in {@code into} win; keys only in
   * {@code from} are added.
   */
  static void mergeSparkConfByKey(Map<String, String> into, Map<String, String> from) {
    String fromJson = from.get("spark_conf");
    if (fromJson == null || fromJson.trim().isEmpty()) {
      return;
    }
    Map<String, String> merged =
        new LinkedHashMap<>(
            IcebergSparkConfigUtils.parseFlatJsonMap(into.get("spark_conf"), "spark_conf"));
    Map<String, String> later = IcebergSparkConfigUtils.parseFlatJsonMap(fromJson, "spark_conf");
    for (Map.Entry<String, String> entry : later.entrySet()) {
      merged.putIfAbsent(entry.getKey(), entry.getValue());
    }
    if (merged.isEmpty()) {
      return;
    }
    try {
      into.put("spark_conf", JsonUtils.anyFieldMapper().writeValueAsString(merged));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to merge spark_conf for commit chain", e);
    }
  }

  /**
   * Union {@link MaintenanceJobTemplates#RULES_KEY} maps so each chain type keeps its own
   * enqueue-time rules for in-Spark recommend.
   */
  static void mergePolicyRulesByType(Map<String, String> into, Map<String, String> from) {
    String fromJson = from.get(MaintenanceJobTemplates.RULES_KEY);
    if (fromJson == null || fromJson.trim().isEmpty()) {
      return;
    }
    Map<String, Map<String, Object>> merged =
        new LinkedHashMap<>(parseRulesByType(into.get(MaintenanceJobTemplates.RULES_KEY)));
    Map<String, Map<String, Object>> later = parseRulesByType(fromJson);
    for (Map.Entry<String, Map<String, Object>> entry : later.entrySet()) {
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

  private static Map<String, Map<String, Object>> parseRulesByType(String json) {
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
}
