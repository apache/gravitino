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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.policy.MaintenanceMinIntervalConfigs;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
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
 * <p>Single-table {@code table:...} rows submit one Spark job. Snapshot-expiry {@code batch:...}
 * rows are one cron task carrying multiple table ids; the pipeline claims each eligible table and
 * submits one single-table Spark job per table (builtin expire-snapshots template unchanged).
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
  private final Function<List<Long>, List<ResolvedTableRef>> tableRefResolver;

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
    this(
        policyLookup,
        occupancyGate,
        actionMetaService,
        jobSubmitter,
        gravitinoConfig,
        claimTimeoutMs,
        MaintenanceTableRefs::resolve);
  }

  /**
   * Visible for tests that inject table-id resolution without a JDBC store.
   *
   * @param tableRefResolver resolves entity ids to catalog + schema.table
   */
  MaintenanceSparkSubmitPipeline(
      PolicyLookup policyLookup,
      ExpandOccupancyGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitter jobSubmitter,
      Config gravitinoConfig,
      long claimTimeoutMs,
      Function<List<Long>, List<ResolvedTableRef>> tableRefResolver) {
    this.policyLookup = Preconditions.checkNotNull(policyLookup);
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobSubmitter = Preconditions.checkNotNull(jobSubmitter);
    this.gravitinoConfig = Preconditions.checkNotNull(gravitinoConfig);
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.claimTimeoutMs = claimTimeoutMs;
    this.tableRefResolver = Preconditions.checkNotNull(tableRefResolver);
  }

  /**
   * Runs one cron-job pick for {@code instanceId}.
   *
   * @param instanceId db-scheduler cron-job instance id
   * @param data task payload; required for {@code batch:...} rows
   */
  public void submit(String instanceId, @Nullable CronJobData data) {
    Optional<CronJobIds.TablePolicyIds> tableIds = CronJobIds.parseTable(instanceId);
    if (tableIds.isPresent()) {
      submitOne(tableIds.get().tableId(), tableIds.get().policyId());
      return;
    }
    Optional<CronJobIds.BatchPolicyIds> batchIds = CronJobIds.parseBatch(instanceId);
    if (batchIds.isPresent()) {
      List<Long> tables = data == null ? List.of() : data.tableIds();
      if (tables.isEmpty()) {
        LOG.warn("Skipping cron-job submit for batch {} with empty task_data", instanceId);
        return;
      }
      submitBatch(tables, batchIds.get().policyId());
      return;
    }
    LOG.warn("Skipping cron-job submit for unparseable instance id {}", instanceId);
  }

  /**
   * Backward-compatible entry for tests that only pass the instance id.
   *
   * @param instanceId db-scheduler cron-job instance id
   */
  public void submit(String instanceId) {
    submit(instanceId, null);
  }

  private void submitOne(long tableId, long policyId) {
    Optional<PolicyEntity> loaded = loadRunnablePolicy(policyId);
    if (loaded.isEmpty()) {
      return;
    }
    PolicyEntity policy = loaded.get();
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

    Map<String, String> jobConf = buildJobConf(policy);
    jobConf.put(TMS_TABLE_ID_KEY, Long.toString(tableId));
    runAndBind(policy, templateName.get(), jobConf, claim.get(), tableId);
  }

  /**
   * One cron batch task, multiple tables: claim each eligible table and submit one single-table
   * Spark job per table (same builtin expire-snapshots template as {@code table:...} rows).
   */
  private void submitBatch(List<Long> tableIds, long policyId) {
    Optional<PolicyEntity> loaded = loadRunnablePolicy(policyId);
    if (loaded.isEmpty()) {
      return;
    }
    PolicyEntity policy = loaded.get();
    if (policy.policyType() != Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      LOG.warn(
          "Skipping batch cron-job submit for policy {} with non-snapshot type {}",
          policyId,
          policy.policyType());
      return;
    }
    Optional<String> templateName = MaintenanceJobTemplates.templateName(policy.policyType());
    if (templateName.isEmpty()) {
      LOG.warn("Skipping batch cron-job submit for policy {} without template", policyId);
      return;
    }

    long minIntervalMs =
        MaintenanceMinIntervalConfigs.resolve(
            gravitinoConfig, policy.policyType(), policyMinIntervalMs(policy.content()));
    long nowMs = System.currentTimeMillis();
    List<Long> eligible = new ArrayList<>();
    for (Long tableId : tableIds) {
      if (occupancyGate.shouldSkip(tableId, policyId, minIntervalMs, nowMs)) {
        LOG.info(
            "Skipping table {} in batch submit for policy {} (in-flight or within minIntervalMs={})",
            tableId,
            policyId,
            minIntervalMs);
        continue;
      }
      eligible.add(tableId);
    }
    if (eligible.isEmpty()) {
      LOG.info("Skipping batch cron-job submit for policy {}: no eligible tables", policyId);
      return;
    }

    List<ResolvedTableRef> resolved = tableRefResolver.apply(eligible);
    if (resolved.isEmpty()) {
      LOG.warn(
          "Skipping batch cron-job submit for policy {}: could not resolve table identifiers {}",
          policyId,
          eligible);
      return;
    }

    String template = templateName.get();
    for (ResolvedTableRef ref : resolved) {
      Optional<OccupancyClaim> claim =
          actionMetaService.tryAcquireInFlight(ref.tableId(), policyId, claimTimeoutMs);
      if (claim.isEmpty()) {
        LOG.info(
            "Skipping table {} in batch submit for policy {} (lost soft-slot race)",
            ref.tableId(),
            policyId);
        continue;
      }

      Map<String, String> jobConf = buildJobConf(policy);
      jobConf.put("catalog_name", ref.catalogName());
      jobConf.put("table_identifier", ref.tableIdentifier());
      jobConf.put(TMS_TABLE_ID_KEY, Long.toString(ref.tableId()));
      runAndBind(policy, template, jobConf, claim.get(), ref.tableId());
    }
  }

  private void runAndBind(
      PolicyEntity policy,
      String templateName,
      Map<String, String> jobConf,
      OccupancyClaim claim,
      long tableId) {
    String metalake = policy.namespace().level(0);
    try {
      long jobId = jobSubmitter.runJob(metalake, templateName, jobConf);
      actionMetaService.bindJobId(claim, jobId);
      LOG.info(
          "Submitted cron-job for table {} policy {} as job {} (template={})",
          tableId,
          policy.id(),
          jobId,
          templateName);
    } catch (RuntimeException e) {
      releaseClaimQuietly(claim);
      LOG.warn(
          "Failed cron-job submit for table {} policy {} (template={}): {}",
          tableId,
          policy.id(),
          templateName,
          e.toString());
    }
  }

  private Optional<PolicyEntity> loadRunnablePolicy(long policyId) {
    Optional<PolicyEntity> loaded = policyLookup.findById(policyId);
    if (loaded.isEmpty()) {
      LOG.info("Skipping cron-job submit for missing policy {}", policyId);
      return Optional.empty();
    }
    PolicyEntity policy = loaded.get();
    if (!policy.enabled() || !isMaintenance(policy)) {
      LOG.info(
          "Skipping cron-job submit for policy {} (enabled={}, maintenance={})",
          policyId,
          policy.enabled(),
          isMaintenance(policy));
      return Optional.empty();
    }
    return Optional.of(policy);
  }

  private Map<String, String> buildJobConf(PolicyEntity policy) {
    PolicyExecutionSpec spec = executionSpec(policy.content());
    Map<String, String> jobOptions =
        MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, spec.jobOptions());
    Map<String, String> updaterOptions =
        MaintenanceAuthConfigs.overlayUpdaterOptions(gravitinoConfig, spec.updaterOptions());

    Map<String, String> jobConf = new LinkedHashMap<>(jobOptions);
    jobConf.put(TMS_POLICY_ID_KEY, Long.toString(policy.id()));
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
