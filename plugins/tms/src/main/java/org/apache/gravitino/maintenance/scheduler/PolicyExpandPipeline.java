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

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expands a due {@code policy-expand} lease into one-shot {@code cron-job} rows (§5.2.1).
 *
 * <p>Does not submit Spark. Returns the next expand due time, or empty when the expand row should
 * be removed (missing / disabled / blank trigger / non-maintenance).
 *
 * <p>Enumeration fills up to {@code enqueueBatchSize} tables that pass nearest-policy and {@code
 * minIntervalMs} / in-flight gating (policy content → server conf → code default).
 */
public final class PolicyExpandPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandPipeline.class);

  private final PolicyLookup policyLookup;
  private final PolicyTableEnumerator tableEnumerator;
  private final CronJobScheduler cronJobScheduler;
  private final ExpandOccupancyGate occupancyGate;
  private final Config config;
  private final int enqueueBatchSize;

  /**
   * @param policyLookup loads the policy for the expand instance id
   * @param tableEnumerator lists attached table ids
   * @param cronJobScheduler writes cron-job rows
   * @param occupancyGate in-flight / minInterval gate
   * @param config gravitino server config for per-type minInterval fallbacks
   * @param enqueueBatchSize max cron-jobs per expand pick
   */
  public PolicyExpandPipeline(
      PolicyLookup policyLookup,
      PolicyTableEnumerator tableEnumerator,
      CronJobScheduler cronJobScheduler,
      ExpandOccupancyGate occupancyGate,
      Config config,
      int enqueueBatchSize) {
    this.policyLookup = policyLookup;
    this.tableEnumerator = tableEnumerator;
    this.cronJobScheduler = cronJobScheduler;
    this.occupancyGate = occupancyGate;
    this.config = config;
    this.enqueueBatchSize = enqueueBatchSize;
  }

  /**
   * Runs one expand pick for {@code policyId}.
   *
   * @param policyId expand task instance id
   * @return next due time when the expand row should stay, or empty to delete the row
   */
  public Optional<Instant> expand(long policyId) {
    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.info("Removing expand row for missing policy {}", policyId);
      return Optional.empty();
    }
    PolicyEntity policy = loaded.get();
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
      nextDue = PolicyExpandCrons.nextExecution(timeTrigger);
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
    List<Long> toEnqueue =
        tableEnumerator.listTableIds(policy, enqueueBatchSize, occupancyGate, minIntervalMs);
    int scheduled = cronJobScheduler.enqueueDueNow(policyId, toEnqueue, enqueueBatchSize);
    LOG.info(
        "Expanded policy {}: enqueued {}/{} cron-jobs (minIntervalMs={}); next expand due {}",
        policyId,
        scheduled,
        toEnqueue.size(),
        minIntervalMs,
        nextDue);
    return Optional.of(nextDue);
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
