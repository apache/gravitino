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
package org.apache.gravitino.maintenance.scheduler.commit;

import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.ChainStep;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobSubmitter;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit-pool control-plane submit (§5.2.3): consume enqueue-time {@link MaintenanceJobSnapshot} →
 * abort if any step is in-flight → soft-slot claim on the first step → one chained {@code runJob} →
 * bind.
 *
 * <p>Chain collection, cooldown filtering, auth consistency, and jobConf merge happen at enqueue
 * (see {@link RelationalCommitJobScheduler}). The caller always deletes the one-shot {@code
 * commit-job} row after this method returns.
 */
public final class MaintenanceCommitSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCommitSubmitPipeline.class);

  private final CommitOccupancyGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitter jobSubmitter;
  private final long claimTimeoutMs;

  /**
   * @param occupancyGate in-flight checks
   * @param actionMetaService soft-slot claim / bind
   * @param jobSubmitter Gravitino Job submitter
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceCommitSubmitPipeline(
      CommitOccupancyGate occupancyGate,
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
   * Runs one commit-job pick for {@code instanceId} ({@code {table_id}}).
   *
   * @param instanceId db-scheduler commit-job instance id
   * @param snapshot enqueue-time payload; required
   */
  public void submit(String instanceId, @Nullable MaintenanceJobSnapshot snapshot) {
    long tableId;
    try {
      tableId = Long.parseLong(instanceId);
    } catch (NumberFormatException e) {
      LOG.warn("Skipping commit-job submit for unparseable instance id {}", instanceId);
      return;
    }
    if (tableId <= 0) {
      LOG.warn("Skipping commit-job submit for non-positive table id {}", instanceId);
      return;
    }
    if (snapshot == null || !snapshot.isValid()) {
      LOG.warn(
          "Skipping commit-job submit for table {} with missing/invalid task_data snapshot",
          tableId);
      return;
    }
    TableTarget target = snapshot.getTables().get(0);
    if (target.getTableId() != tableId) {
      LOG.warn(
          "Skipping commit-job submit for instance {} whose snapshot tableId is {}",
          instanceId,
          target.getTableId());
      return;
    }

    // Design §5.2.3: any remaining type in-flight → delete commit-job and stop.
    for (ChainStep step : snapshot.getSteps()) {
      if (occupancyGate.isInFlight(tableId, step.getPolicyId())) {
        LOG.info(
            "Skipping commit-job submit for table {} (type {} policy {} still in-flight)",
            tableId,
            step.getMaintenanceType(),
            step.getPolicyId());
        return;
      }
    }

    ChainStep first = snapshot.getSteps().get(0);
    Optional<OccupancyClaim> claim =
        actionMetaService.tryAcquireInFlight(tableId, first.getPolicyId(), claimTimeoutMs);
    if (claim.isEmpty()) {
      LOG.info(
          "Skipping commit-job submit for table {} (lost soft-slot race on first policy {})",
          tableId,
          first.getPolicyId());
      return;
    }

    runAndBind(snapshot, claim.get(), tableId);
  }

  private void runAndBind(MaintenanceJobSnapshot snapshot, OccupancyClaim claim, long tableId) {
    String metalake = snapshot.getMetalake();
    Map<String, String> jobConf = new LinkedHashMap<>(snapshot.getJobConf());
    Long jobId = null;
    try {
      jobId =
          jobSubmitter.runJob(
              metalake, MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
      actionMetaService.bindJobId(claim, jobId);
      LOG.info(
          "Submitted commit-job for table {} as job {} (chain={}, firstPolicy={})",
          tableId,
          jobId,
          snapshot.getSteps().stream()
              .map(ChainStep::getMaintenanceType)
              .collect(Collectors.toList()),
          snapshot.getSteps().get(0).getPolicyId());
    } catch (RuntimeException e) {
      if (jobId != null) {
        cancelJobQuietly(metalake, jobId);
      }
      releaseClaimQuietly(claim);
      LOG.warn(
          "Failed commit-job submit for table {} firstPolicy={}: {}",
          tableId,
          snapshot.getSteps().get(0).getPolicyId(),
          e.toString());
    }
  }

  private void cancelJobQuietly(String metalake, long jobId) {
    try {
      jobSubmitter.cancelJob(metalake, jobId);
    } catch (RuntimeException cancelError) {
      LOG.warn(
          "Failed to cancel orphaned commit Spark job {} under metalake {} after bind failure",
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
          "Failed to release occupancy claim {} after commit submit failure",
          claim.id(),
          releaseError);
    }
  }
}
