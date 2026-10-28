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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Operation;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.TableTarget;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobSubmitter;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit-pool control-plane submit (§5.2.3): consume enqueue-time {@link MaintenanceJobSpec} →
 * abort if any step is in-flight → soft-slot claim for every chain policy → one chained {@code
 * runJob} → bind the same job id on every claim.
 *
 * <p>Chain collection, cooldown filtering, auth consistency, and jobConf merge happen at enqueue
 * (see {@link CommitJobClient}). The caller always deletes the one-shot {@code commit-job} row
 * after this method returns.
 */
public final class MaintenanceCommitSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCommitSubmitPipeline.class);

  private final CommitOccupancyGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitter jobSubmitter;
  private final MaintenanceJobConfBuilder jobConfBuilder;
  private final long claimTimeoutMs;

  /**
   * @param occupancyGate in-flight checks
   * @param actionMetaService soft-slot claim / bind
   * @param jobSubmitter Gravitino Job submitter
   * @param jobConfBuilder resolves enqueue-time conf-key refs before {@code runJob}
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceCommitSubmitPipeline(
      CommitOccupancyGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitter jobSubmitter,
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
   * Runs one commit-job pick for {@code instanceId} ({@code table:{table_id}}).
   *
   * @param instanceId db-scheduler commit-job instance id
   * @param spec enqueue-time payload; required
   */
  public void submit(String instanceId, @Nullable MaintenanceJobSpec spec) {
    Optional<Long> parsed = CommitJobIds.parseTable(instanceId);
    if (parsed.isEmpty()) {
      LOG.warn("Skipping commit-job submit for unparseable instance id {}", instanceId);
      return;
    }
    long tableId = parsed.get();
    if (tableId <= 0) {
      LOG.warn("Skipping commit-job submit for non-positive table id {}", instanceId);
      return;
    }
    if (spec == null || !spec.isValid()) {
      LOG.warn(
          "Skipping commit-job submit for table {} with missing/invalid task_data spec", tableId);
      return;
    }
    TableTarget target = spec.getTables().get(0);
    if (target.getTableId() != tableId) {
      LOG.warn(
          "Skipping commit-job submit for instance {} whose spec tableId is {}",
          instanceId,
          target.getTableId());
      return;
    }

    // Design §5.2.3: any remaining type in-flight → delete commit-job and stop.
    for (Operation step : spec.getOperations()) {
      if (occupancyGate.isInFlight(tableId, step.getPolicyId())) {
        LOG.info(
            "Skipping commit-job submit for table {} (type {} policy {} still in-flight)",
            tableId,
            step.getMaintenanceType(),
            step.getPolicyId());
        return;
      }
    }

    List<Long> policyIds =
        spec.getOperations().stream().map(Operation::getPolicyId).collect(Collectors.toList());
    Optional<List<OccupancyClaim>> claims =
        actionMetaService.tryAcquireInFlightBatch(tableId, policyIds, claimTimeoutMs);
    if (claims.isEmpty()) {
      LOG.info(
          "Skipping commit-job submit for table {} (lost soft-slot race on chain policies {})",
          tableId,
          policyIds);
      return;
    }

    runAndBind(spec, claims.get(), tableId);
  }

  private void runAndBind(MaintenanceJobSpec spec, List<OccupancyClaim> claims, long tableId) {
    String metalake = spec.metalake();
    Map<String, String> jobConf;
    try {
      jobConf =
          jobConfBuilder.resolveForSubmit(
              MaintenanceJobSpecFactory.jobConfForTable(
                  spec.getJobConf(), spec.getTables().get(0)));
    } catch (RuntimeException e) {
      actionMetaService.deleteClaims(claims);
      LOG.warn(
          "Skipping commit-job submit for table {}: invalid spec table target: {}",
          tableId,
          e.toString());
      return;
    }
    Long jobId = null;
    try {
      jobId =
          jobSubmitter.runJob(
              metalake, MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE, jobConf);
      actionMetaService.bindJobIds(claims, jobId);
      LOG.info(
          "Submitted commit-job for table {} as job {} (chain={}, policies={})",
          tableId,
          jobId,
          spec.getOperations().stream()
              .map(Operation::getMaintenanceType)
              .collect(Collectors.toList()),
          spec.getOperations().stream().map(Operation::getPolicyId).collect(Collectors.toList()));
    } catch (RuntimeException e) {
      if (jobId != null) {
        cancelJobQuietly(metalake, jobId);
      }
      actionMetaService.deleteClaims(claims);
      LOG.warn(
          "Failed commit-job submit for table {} policies={}: {}",
          tableId,
          spec.getOperations().stream().map(Operation::getPolicyId).collect(Collectors.toList()),
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
}
