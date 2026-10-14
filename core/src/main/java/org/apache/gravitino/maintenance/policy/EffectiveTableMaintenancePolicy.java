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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Effective scheduling / submit view for a table and built-in maintenance policy type.
 *
 * <p>Carries the nearest policy for triggers and resolved {@code minIntervalMs}, plus {@code
 * jobOptions} / {@code updateOptions} merged across the metadata hierarchy (catalog → schema →
 * table). Missing keys on a nearer attachment are filled from outer levels; the same key on a
 * nearer policy overrides an outer value.
 */
public final class EffectiveTableMaintenancePolicy {

  private final PolicyEntity policy;
  private final Policy.BuiltInType builtInType;
  @Nullable private final String timeTrigger;
  @Nullable private final Boolean commitTrigger;
  private final long minIntervalMs;
  private final Map<String, String> jobOptions;
  private final Map<String, String> updateOptions;

  /**
   * Creates an effective policy view.
   *
   * @param policy nearest policy entity
   * @param builtInType built-in maintenance policy type
   * @param timeTrigger cron expression from the nearest policy content, or null when unset
   * @param commitTrigger commit-trigger flag from the nearest policy content, or null when unset
   * @param minIntervalMs resolved minimum interval
   * @param jobOptions hierarchy-merged job options
   * @param updateOptions hierarchy-merged update options
   */
  public EffectiveTableMaintenancePolicy(
      PolicyEntity policy,
      Policy.BuiltInType builtInType,
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updateOptions) {
    this.policy = Preconditions.checkNotNull(policy, "policy");
    this.builtInType = Preconditions.checkNotNull(builtInType, "builtInType");
    this.timeTrigger = timeTrigger;
    this.commitTrigger = commitTrigger;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = copyOptions(jobOptions);
    this.updateOptions = copyOptions(updateOptions);
  }

  /**
   * @return nearest policy entity
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return built-in maintenance policy type
   */
  public Policy.BuiltInType builtInType() {
    return builtInType;
  }

  /**
   * @return cron expression from the nearest policy content, or null when unset
   */
  @Nullable
  public String timeTrigger() {
    return timeTrigger;
  }

  /**
   * @return commit-trigger flag from the nearest policy content, or null when unset
   */
  @Nullable
  public Boolean commitTrigger() {
    return commitTrigger;
  }

  /**
   * @return resolved minimum interval between runs
   */
  public long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return hierarchy-merged job options; never {@code null}
   */
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  /**
   * @return hierarchy-merged update options; never {@code null}
   */
  public Map<String, String> updateOptions() {
    return updateOptions;
  }

  /**
   * Resolves effective TMS policy for a metadata object.
   *
   * <p>{@code timeTrigger}, {@code commitTrigger}, and {@code minIntervalMs} come from the nearest
   * matching policy. {@code jobOptions} / {@code updateOptions} are the union of direct matches
   * along catalog → schema → table (nearer keys win).
   *
   * @param resolver nearest-policy resolver
   * @param metalake metalake name
   * @param metadataObject metadata object
   * @param builtInType built-in maintenance policy type
   * @param config gravitino server configuration
   * @return effective policy when a matching policy exists
   */
  public static Optional<EffectiveTableMaintenancePolicy> resolve(
      NearestMaintenancePolicyResolver resolver,
      String metalake,
      MetadataObject metadataObject,
      Policy.BuiltInType builtInType,
      Config config) {
    List<PolicyEntity> chain = resolver.resolveDirectChain(metalake, metadataObject, builtInType);
    if (chain.isEmpty()) {
      return Optional.empty();
    }

    PolicyEntity nearest = chain.get(0);
    PolicyContent nearestContent = nearest.content();
    long interval =
        MaintenanceMinIntervalConfigs.resolve(config, builtInType, nearestContent.minIntervalMs());
    Map<String, String> mergedJobOptions = mergeOptions(chain, true);
    Map<String, String> mergedUpdateOptions = mergeOptions(chain, false);
    return Optional.of(
        new EffectiveTableMaintenancePolicy(
            nearest,
            builtInType,
            nearestContent.timeTrigger(),
            nearestContent.commitTrigger(),
            interval,
            mergedJobOptions,
            mergedUpdateOptions));
  }

  /**
   * Merges option maps from farthest to nearest so missing nearer keys are filled from outer
   * policies and nearer values override on conflict.
   */
  private static Map<String, String> mergeOptions(
      List<PolicyEntity> nearestFirstChain, boolean job) {
    Map<String, String> merged = new LinkedHashMap<>();
    for (int i = nearestFirstChain.size() - 1; i >= 0; i--) {
      PolicyContent content = nearestFirstChain.get(i).content();
      Map<String, String> level = job ? content.jobOptions() : content.updateOptions();
      if (level != null && !level.isEmpty()) {
        merged.putAll(level);
      }
    }
    return merged.isEmpty() ? Collections.emptyMap() : ImmutableMap.copyOf(merged);
  }

  private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Collections.emptyMap();
    }
    return ImmutableMap.copyOf(options);
  }
}
