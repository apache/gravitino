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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.MaintenanceJobSubmitService;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cron-pool control-plane submit (§5.2.2): consume enqueue-time {@link MaintenanceJobSpec} →
 * in-flight gate → soft-slot claim → {@code runJob} → bind job id.
 *
 * <p>Single-table {@code table:...} and snapshot-expiry {@code batch:...} rows share the same spec
 * shape; each {@link Task} becomes one Spark job. If any task table is in-flight, the whole pick
 * stops without partial submits (one-shot row is still removed by the caller).
 *
 * <p>Does not sample metrics or evaluate trigger-expr (both run inside Spark from enqueue-time
 * {@code rules} in task parameters).
 */
public final class MaintenanceSparkSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceSparkSubmitPipeline.class);

  private final CronSubmitGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitService jobSubmitter;
  private final MaintenanceJobConfBuilder jobConfBuilder;
  private final long claimTimeoutMs;

  /**
   * @param occupancyGate in-flight gate (minInterval already applied at enqueue)
   * @param actionMetaService soft-slot claim / bind
   * @param jobSubmitter Gravitino Job submitter
   * @param jobConfBuilder resolves enqueue-time conf-key refs before {@code runJob}
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceSparkSubmitPipeline(
      CronSubmitGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitService jobSubmitter,
      MaintenanceJobConfBuilder jobConfBuilder,
      long claimTimeoutMs) {
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobSubmitter = Preconditions.checkNotNull(jobSubmitter);
    this.jobConfBuilder = Preconditions.checkNotNull(jobConfBuilder);
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * Runs one cron-job pick for {@code instanceId}.
   *
   * @param instanceId db-scheduler cron-job instance id
   * @param spec enqueue-time payload; required
   */
  public void submit(String instanceId, @Nullable MaintenanceJobSpec spec) {
    if (spec == null || !spec.isValid()) {
      LOG.warn("Skipping cron-job submit for {} with missing/invalid task_data spec", instanceId);
      return;
    }

    Optional<long[]> tableIds = MaintenanceTaskIds.parseCronTable(instanceId);
    Optional<long[]> batchIds = MaintenanceTaskIds.parseCronBatch(instanceId);
    if (tableIds.isEmpty() && batchIds.isEmpty()) {
      LOG.warn("Skipping cron-job submit for unparseable instance id {}", instanceId);
      return;
    }

    long policyId = spec.getTasks().get(0).getPolicyId();
    // parseCron* returns [leftId, policyId]
    long expectedPolicyId = tableIds.isPresent() ? tableIds.get()[1] : batchIds.get()[1];
    if (policyId != expectedPolicyId) {
      LOG.warn(
          "Skipping cron-job submit for {}: spec policyId {} != instance policyId {}",
          instanceId,
          policyId,
          expectedPolicyId);
      return;
    }
    for (Task task : spec.getTasks()) {
      if (task.getPolicyId() != policyId) {
        LOG.warn(
            "Skipping cron-job submit for {}: mixed policyIds in tasks ({} vs {})",
            instanceId,
            policyId,
            task.getPolicyId());
        return;
      }
    }
    if (tableIds.isPresent()) {
      long expectedTableId = tableIds.get()[0];
      if (spec.getTasks().size() != 1 || spec.getTasks().get(0).getTableId() != expectedTableId) {
        LOG.warn(
            "Skipping cron-job submit for {}: spec tables {} do not match instance tableId {}",
            instanceId,
            spec.tableIds(),
            expectedTableId);
        return;
      }
    }

    // minInterval was gated at expand enqueue; pick only blocks on in-flight. Any in-flight
    // member aborts the whole pick so a batch row is not partially submitted then deleted.
    for (Task task : spec.getTasks()) {
      if (occupancyGate.isInFlight(task.getTableId(), policyId)) {
        LOG.info(
            "Skipping cron-job {} entirely (table {} in-flight for policy {})",
            instanceId,
            task.getTableId(),
            policyId);
        return;
      }
    }
    for (Task task : spec.getTasks()) {
      submitOne(spec, task);
    }
  }

  private void submitOne(MaintenanceJobSpec spec, Task task) {
    long policyId = task.getPolicyId();
    Optional<OccupancyClaim> claim =
        actionMetaService.tryAcquireInFlight(task.getTableId(), policyId, claimTimeoutMs);
    if (claim.isEmpty()) {
      LOG.info(
          "Skipping cron-job submit for table {} policy {} (lost soft-slot race)",
          task.getTableId(),
          policyId);
      return;
    }

    Map<String, String> jobConf;
    try {
      jobConf =
          jobConfBuilder.resolveForSubmit(MaintenanceJobSpecFactory.jobConfForTask(spec, task));
    } catch (RuntimeException e) {
      releaseClaimQuietly(claim.get());
      LOG.warn(
          "Skipping cron-job submit for table {} policy {}: invalid task: {}",
          task.getTableId(),
          policyId,
          e.toString());
      return;
    }
    runAndBind(spec.metalake(), jobConf, claim.get(), task.getTableId(), policyId);
  }

  private void runAndBind(
      String metalake,
      Map<String, String> jobConf,
      OccupancyClaim claim,
      long tableId,
      long policyId) {
    jobConf.put(MaintenanceJobTemplates.ACTION_IDS_KEY, Long.toString(claim.actionId()));
    Long jobId = null;
    try {
      jobId =
          jobSubmitter.runJob(
              metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
      actionMetaService.bindJobId(claim, jobId);
      LOG.info("Submitted cron-job for table {} policy {} as job {}", tableId, policyId, jobId);
    } catch (RuntimeException e) {
      if (jobId != null) {
        cancelJobQuietly(metalake, jobId);
      }
      releaseClaimQuietly(claim);
      LOG.warn(
          "Failed cron-job submit for table {} policy {}: {}", tableId, policyId, e.toString());
    }
  }

  private void cancelJobQuietly(String metalake, long jobId) {
    try {
      jobSubmitter.cancelJob(metalake, jobId);
    } catch (RuntimeException cancelError) {
      LOG.warn(
          "Failed to cancel orphaned cron Spark job {} under metalake {} after bind failure",
          jobId,
          metalake,
          cancelError);
    }
  }

  private void releaseClaimQuietly(OccupancyClaim claim) {
    try {
      // Fence on (id, created_at) + unbound so a concurrent reclaim's live claim is not deleted.
      if (!actionMetaService.deleteUnboundClaim(claim)) {
        LOG.debug(
            "Occupancy claim {} created_at={} was not released (reclaimed or already bound)",
            claim.actionId(),
            claim.createdAt());
      }
    } catch (RuntimeException releaseError) {
      LOG.warn(
          "Failed to release occupancy claim {} after submit failure",
          claim.actionId(),
          releaseError);
    }
  }
}
