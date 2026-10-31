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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.MaintenanceActionManager;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.maintenance.scheduler.TableCandidate;
import org.apache.gravitino.maintenance.scheduler.job.cron.CronJobClient;
import org.apache.gravitino.maintenance.scheduler.policy.NearestPolicyTableEnumerator;
import org.apache.gravitino.maintenance.scheduler.policy.NearestPolicyTableEnumerator.ShouldSkipTable;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans one {@code policy-expand} pick (§5.2.1): decide whether the lease stays, enumerate
 * nearest-policy tables, and schedule {@code cron-job} rows via {@link CronJobClient}.
 *
 * <p>Parallel to {@code CronJobPlanner} / {@code CommitJobPlanner}. Expand planning runs at pick
 * time (unlike cron/commit, which plan at schedule). Does not submit Spark.
 */
public final class PolicyExpandPlanner {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandPlanner.class);
  private static final PolicyMetaService POLICY_SERVICE = PolicyMetaService.getInstance();

  private final NearestPolicyTableEnumerator tableEnumerator;
  private final CronJobClient cronJobClient;
  private final ShouldSkipTable shouldSkip;
  private final Config config;
  private final int scheduleBatchSize;

  /**
   * Loads policies through {@link PolicyMetaService}.
   *
   * @param tableEnumerator lists attached table ids
   * @param cronJobClient writes cron-job rows
   * @param actionManager decides whether a maintenance action can start
   * @param config gravitino server config for per-type minInterval fallbacks
   * @param scheduleBatchSize max cron-jobs per expand pick
   */
  public PolicyExpandPlanner(
      NearestPolicyTableEnumerator tableEnumerator,
      CronJobClient cronJobClient,
      MaintenanceActionManager actionManager,
      Config config,
      int scheduleBatchSize) {
    this.tableEnumerator = Preconditions.checkNotNull(tableEnumerator);
    this.cronJobClient = Preconditions.checkNotNull(cronJobClient);
    this.shouldSkip = expandShouldSkip(actionManager, cronJobClient);
    this.config = Preconditions.checkNotNull(config);
    this.scheduleBatchSize = scheduleBatchSize;
  }

  /**
   * Plans one expand pick for {@code policyId}.
   *
   * @param policyId expand task instance id
   * @return next due time when the expand row should stay, or empty to delete the row
   */
  public Optional<Instant> plan(long policyId) {
    PolicyEntity policy = POLICY_SERVICE.getPolicyById(policyId);
    if (policy == null) {
      LOG.info("Removing expand row for missing policy {}", policyId);
      return Optional.empty();
    }
    if (!policy.enabled() || !isMaintenance(policy.content())) {
      LOG.info(
          "Removing expand row for policy {} (enabled={}, maintenance={})",
          policyId,
          policy.enabled(),
          isMaintenance(policy.content()));
      return Optional.empty();
    }
    String timeTrigger = timeTrigger(policy.content());
    if (timeTrigger == null || timeTrigger.isBlank()) {
      LOG.info("Removing expand row for policy {} with blank timeTrigger", policyId);
      return Optional.empty();
    }

    // Resolve next due before any cron-job writes so a bad crontab cannot leave orphans.
    final Instant nextDue;
    try {
      nextDue = PolicyExpandClient.nextExecution(timeTrigger);
    } catch (RuntimeException e) {
      LOG.warn(
          "Removing expand row for policy {} with invalid timeTrigger {}: {}",
          policyId,
          timeTrigger,
          e.toString());
      return Optional.empty();
    }

    long minIntervalMs =
        MaintenanceMinIntervalConfigs.resolve(
            config, policy.policyType(), policyMinIntervalMs(policy.content()));
    List<TableCandidate> toSchedule =
        tableEnumerator.listTableIds(policy, scheduleBatchSize, shouldSkip, minIntervalMs);
    int scheduled = cronJobClient.scheduleNow(policy, toSchedule, scheduleBatchSize);
    LOG.info(
        "Expanded policy {}: scheduled {} cron-job rows for {} tables (type={}, minIntervalMs={}); next expand due {}",
        policyId,
        scheduled,
        toSchedule.size(),
        policy.policyType(),
        minIntervalMs,
        nextDue);
    return Optional.of(nextDue);
  }

  private static ShouldSkipTable expandShouldSkip(
      MaintenanceActionManager actionManager, CronJobClient cronJobClient) {
    Preconditions.checkNotNull(actionManager);
    Preconditions.checkNotNull(cronJobClient);
    return (tableId, policyId, minIntervalMs, nowMs) ->
        !actionManager.canStart(tableId, policyId, minIntervalMs, nowMs)
            || cronJobClient.isTableAlreadyQueued(tableId, policyId);
  }

  private static boolean isMaintenance(@Nullable PolicyContent content) {
    if (content == null) {
      return false;
    }
    for (Policy.BuiltInType type : Policy.BuiltInType.values()) {
      if (MaintenancePolicyDefinitions.isMaintenance(type)
          && type.contentClass().isInstance(content)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private static String timeTrigger(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().timeTrigger();
  }

  @Nullable
  private static Long policyMinIntervalMs(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().minIntervalMs();
  }
}
