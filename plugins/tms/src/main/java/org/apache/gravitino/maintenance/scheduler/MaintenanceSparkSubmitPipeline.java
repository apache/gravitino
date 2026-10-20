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
import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cron-pool control-plane submit (§5.2.2): occupancy gate → soft-slot claim → {@code runJob} → bind
 * job id.
 *
 * <p>Does not sample metrics (that runs inside Spark). The caller always deletes the one-shot
 * {@code cron-job} row after this method returns.
 */
public final class MaintenanceSparkSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceSparkSubmitPipeline.class);

  /** JobConf key for overlaid updater options (JSON object string). */
  public static final String UPDATER_OPTIONS_JOB_CONF_KEY = "updater_options";

  /** JobConf key carrying the TMS table entity id for in-job follow-ups. */
  public static final String TMS_TABLE_ID_KEY = "tms_table_id";

  /** JobConf key carrying the TMS policy entity id for in-job follow-ups. */
  public static final String TMS_POLICY_ID_KEY = "tms_policy_id";

  private final PolicyLookup policyLookup;
  private final ExpandOccupancyGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitter jobSubmitter;
  private final Config gravitinoConfig;
  private final long claimTimeoutMs;

  /**
   * @param policyLookup loads the policy for the cron instance
   * @param occupancyGate in-flight / minInterval gate
   * @param actionMetaService soft-slot claim / bind
   * @param jobSubmitter Gravitino Job submitter
   * @param gravitinoConfig server config for auth overlay and minInterval fallbacks
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceSparkSubmitPipeline(
      PolicyLookup policyLookup,
      ExpandOccupancyGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitter jobSubmitter,
      Config gravitinoConfig,
      long claimTimeoutMs) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobSubmitter = Preconditions.checkNotNull(jobSubmitter);
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * Runs one cron-job pick for {@code instanceId} ({@code table:{tableId}:{policyId}}).
   *
   * @param instanceId db-scheduler cron-job instance id
   */
  public void submit(String instanceId) {
    Optional<CronJobIds.TablePolicyIds> parsed = CronJobIds.parseTable(instanceId);
    if (parsed.isEmpty()) {
      LOG.warn("Skipping cron-job submit for unparseable instance id {}", instanceId);
      return;
    }
    long tableId = parsed.get().tableId();
    long policyId = parsed.get().policyId();

    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.info("Skipping cron-job submit for missing policy {} (table {})", policyId, tableId);
      return;
    }
    PolicyEntity policy = loaded.get();
    if (!policy.enabled() || !isMaintenance(policy)) {
      LOG.info(
          "Skipping cron-job submit for policy {} (enabled={}, maintenance={})",
          policyId,
          policy.enabled(),
          isMaintenance(policy));
      return;
    }

    Optional<String> templateName = MaintenanceJobTemplates.templateName(policy.policyType());
    if (templateName.isEmpty()) {
      LOG.warn(
          "Skipping cron-job submit for policy {} with unsupported type {}",
          policyId,
          policy.policyType());
      return;
    }

    long minIntervalMs =
        MaintenanceMinIntervalConfigs.resolve(
            gravitinoConfig, policy.policyType(), policyMinIntervalMs(policy.content()));
    long nowMs = System.currentTimeMillis();
    if (occupancyGate.shouldSkip(tableId, policyId, minIntervalMs, nowMs)) {
      LOG.info(
          "Skipping cron-job submit for table {} policy {} (in-flight or within minIntervalMs={})",
          tableId,
          policyId,
          minIntervalMs);
      return;
    }

    Optional<OccupancyClaim> claim =
        actionMetaService.tryAcquireInFlight(tableId, policyId, claimTimeoutMs);
    if (claim.isEmpty()) {
      LOG.info(
          "Skipping cron-job submit for table {} policy {} (lost soft-slot race)",
          tableId,
          policyId);
      return;
    }

    String metalake = policy.namespace().level(0);
    Map<String, String> jobConf = buildJobConf(policy, tableId, policyId);
    try {
      long jobId = jobSubmitter.runJob(metalake, templateName.get(), jobConf);
      actionMetaService.bindJobId(claim.get(), jobId);
      LOG.info(
          "Submitted cron-job for table {} policy {} as job {} (template={})",
          tableId,
          policyId,
          jobId,
          templateName.get());
    } catch (RuntimeException e) {
      releaseClaimQuietly(claim.get());
      LOG.warn(
          "Failed cron-job submit for table {} policy {} (template={}): {}",
          tableId,
          policyId,
          templateName.get(),
          e.toString());
    }
  }

  private Map<String, String> buildJobConf(PolicyEntity policy, long tableId, long policyId) {
    PolicyExecutionSpec spec = executionSpec(policy.content());
    Map<String, String> jobOptions =
        MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, spec.jobOptions());
    Map<String, String> updaterOptions =
        MaintenanceAuthConfigs.overlayUpdaterOptions(gravitinoConfig, spec.updaterOptions());

    Map<String, String> jobConf = new LinkedHashMap<>(jobOptions);
    jobConf.put(TMS_TABLE_ID_KEY, Long.toString(tableId));
    jobConf.put(TMS_POLICY_ID_KEY, Long.toString(policyId));
    if (!updaterOptions.isEmpty()) {
      try {
        jobConf.put(
            UPDATER_OPTIONS_JOB_CONF_KEY,
            JsonUtils.anyFieldMapper().writeValueAsString(updaterOptions));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Failed to serialize updater_options for runJob", e);
      }
    }
    return jobConf;
  }

  private void releaseClaimQuietly(OccupancyClaim claim) {
    try {
      actionMetaService.deleteById(claim.id());
    } catch (RuntimeException releaseError) {
      LOG.warn(
          "Failed to release occupancy claim {} after submit failure", claim.id(), releaseError);
    }
  }

  private static boolean isMaintenance(PolicyEntity policy) {
    return MaintenancePolicyDefinitions.isMaintenance(policy.policyType())
        && isMaintenanceContent(policy.content());
  }

  private static boolean isMaintenanceContent(@Nullable PolicyContent content) {
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

  private static PolicyExecutionSpec executionSpec(@Nullable PolicyContent content) {
    if (content instanceof ExecutablePolicyContent) {
      return ((ExecutablePolicyContent) content).executionSpec();
    }
    return PolicyExecutionSpec.empty();
  }

  @Nullable
  private static Long policyMinIntervalMs(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().minIntervalMs();
  }
}
