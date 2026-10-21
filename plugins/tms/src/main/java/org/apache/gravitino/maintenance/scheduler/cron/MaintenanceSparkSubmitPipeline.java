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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.ChainStep;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshotFactory;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cron-pool control-plane submit (§5.2.2): consume enqueue-time {@link MaintenanceJobSnapshot} →
 * in-flight gate → soft-slot claim → {@code runJob} → bind job id.
 *
 * <p>Single-table {@code table:...} and snapshot-expiry {@code batch:...} rows share the same
 * snapshot shape; batch pick overlays per-table keys and submits one Spark job per table.
 *
 * <p>Does not sample metrics (that runs inside Spark). The caller always deletes the one-shot
 * {@code cron-job} row after this method returns. Policy / jobConf are not recomputed here.
 */
public final class MaintenanceSparkSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceSparkSubmitPipeline.class);

  private final ExpandOccupancyGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitter jobSubmitter;
  private final long claimTimeoutMs;

  /**
   * @param occupancyGate in-flight gate (minInterval already applied at enqueue)
   * @param actionMetaService soft-slot claim / bind
   * @param jobSubmitter Gravitino Job submitter
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceSparkSubmitPipeline(
      ExpandOccupancyGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitter jobSubmitter,
      long claimTimeoutMs) {
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobSubmitter = Preconditions.checkNotNull(jobSubmitter);
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * Runs one cron-job pick for {@code instanceId}.
   *
   * @param instanceId db-scheduler cron-job instance id
   * @param snapshot enqueue-time payload; required
   */
  public void submit(String instanceId, @Nullable MaintenanceJobSnapshot snapshot) {
    if (snapshot == null || !snapshot.isValid()) {
      LOG.warn(
          "Skipping cron-job submit for {} with missing/invalid task_data snapshot", instanceId);
      return;
    }

    Optional<CronJobIds.TablePolicyIds> tableIds = CronJobIds.parseTable(instanceId);
    Optional<CronJobIds.BatchPolicyIds> batchIds = CronJobIds.parseBatch(instanceId);
    if (tableIds.isEmpty() && batchIds.isEmpty()) {
      LOG.warn("Skipping cron-job submit for unparseable instance id {}", instanceId);
      return;
    }

    ChainStep step = snapshot.getSteps().get(0);
    long policyId = step.getPolicyId();
    long expectedPolicyId =
        tableIds.isPresent() ? tableIds.get().policyId() : batchIds.get().policyId();
    if (policyId != expectedPolicyId) {
      LOG.warn(
          "Skipping cron-job submit for {}: snapshot policyId {} != instance policyId {}",
          instanceId,
          policyId,
          expectedPolicyId);
      return;
    }
    if (tableIds.isPresent()) {
      long expectedTableId = tableIds.get().tableId();
      if (snapshot.getTables().size() != 1
          || snapshot.getTables().get(0).getTableId() != expectedTableId) {
        LOG.warn(
            "Skipping cron-job submit for {}: snapshot tables {} do not match instance tableId {}",
            instanceId,
            snapshot.tableIds(),
            expectedTableId);
        return;
      }
    }

    long nowMs = System.currentTimeMillis();
    // minInterval was gated at expand enqueue; pick only blocks on in-flight (minIntervalMs=0).
    for (TableTarget target : snapshot.getTables()) {
      if (occupancyGate.shouldSkip(target.getTableId(), policyId, 0L, nowMs)) {
        LOG.info(
            "Skipping table {} in cron-job {} (in-flight for policy {})",
            target.getTableId(),
            instanceId,
            policyId);
        continue;
      }
      submitOne(snapshot, target, policyId);
    }
  }

  private void submitOne(MaintenanceJobSnapshot snapshot, TableTarget target, long policyId) {
    Optional<OccupancyClaim> claim =
        actionMetaService.tryAcquireInFlight(target.getTableId(), policyId, claimTimeoutMs);
    if (claim.isEmpty()) {
      LOG.info(
          "Skipping cron-job submit for table {} policy {} (lost soft-slot race)",
          target.getTableId(),
          policyId);
      return;
    }

    Map<String, String> jobConf;
    try {
      jobConf = MaintenanceJobSnapshotFactory.jobConfForTable(snapshot.getJobConf(), target);
    } catch (RuntimeException e) {
      releaseClaimQuietly(claim.get());
      LOG.warn(
          "Skipping cron-job submit for table {} policy {}: invalid snapshot table target: {}",
          target.getTableId(),
          policyId,
          e.toString());
      return;
    }
    jobConf.putIfAbsent(MaintenanceJobTemplates.TMS_POLICY_ID_KEY, Long.toString(policyId));
    runAndBind(snapshot.getMetalake(), jobConf, claim.get(), target.getTableId(), policyId);
  }

  private void runAndBind(
      String metalake,
      Map<String, String> jobConf,
      OccupancyClaim claim,
      long tableId,
      long policyId) {
    Long jobId = null;
    try {
      jobId =
          jobSubmitter.runJob(
              metalake, MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
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
      actionMetaService.deleteById(claim.id());
    } catch (RuntimeException releaseError) {
      LOG.warn(
          "Failed to release occupancy claim {} after submit failure", claim.id(), releaseError);
    }
  }
}
