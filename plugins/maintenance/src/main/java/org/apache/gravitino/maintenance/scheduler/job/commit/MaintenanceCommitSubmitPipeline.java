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
import org.apache.gravitino.maintenance.MaintenanceJobSubmitManager;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit-pool control-plane submit (§5.2.3): consume enqueue-time {@link MaintenanceJobSpec} →
 * abort if any step is in-flight → soft-slot claim for every chain policy → {@link
 * MaintenanceJobSubmitManager#submitAndBind} for one chained Spark job.
 *
 * <p>Chain collection, cooldown filtering, auth consistency, and task_data assembly happen at
 * enqueue (see {@link CommitJobClient}). The caller always deletes the one-shot {@code commit-job}
 * row after this method returns.
 */
public final class MaintenanceCommitSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCommitSubmitPipeline.class);

  private final CommitOccupancyGate occupancyGate;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobSubmitManager jobSubmitManager;
  private final MaintenanceJobConfBuilder jobConfBuilder;
  private final long claimTimeoutMs;

  /**
   * @param occupancyGate in-flight checks
   * @param actionMetaService soft-slot claim
   * @param jobSubmitManager runJob + bind (and orphan cancel)
   * @param jobConfBuilder resolves enqueue-time conf-key refs before submit
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceCommitSubmitPipeline(
      CommitOccupancyGate occupancyGate,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobSubmitManager jobSubmitManager,
      MaintenanceJobConfBuilder jobConfBuilder,
      long claimTimeoutMs) {
    this.occupancyGate = Preconditions.checkNotNull(occupancyGate);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobSubmitManager = Preconditions.checkNotNull(jobSubmitManager);
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
    Optional<Long> parsed = MaintenanceTaskIds.parseCommitTable(instanceId);
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
    if (spec.getTasks().stream().anyMatch(task -> task.getTableId() != tableId)) {
      LOG.warn(
          "Skipping commit-job submit for instance {} whose spec tableIds are {}",
          instanceId,
          spec.tableIds());
      return;
    }

    // Design §5.2.3: any remaining type in-flight → delete commit-job and stop.
    for (Task task : spec.getTasks()) {
      if (occupancyGate.isInFlight(tableId, task.getPolicyId())) {
        LOG.info(
            "Skipping commit-job submit for table {} (type {} policy {} still in-flight)",
            tableId,
            task.getMaintenanceType(),
            task.getPolicyId());
        return;
      }
    }

    List<Long> policyIds =
        spec.getTasks().stream().map(Task::getPolicyId).collect(Collectors.toList());
    Optional<List<OccupancyClaim>> claims =
        actionMetaService.tryAcquireInFlightBatch(tableId, policyIds, claimTimeoutMs);
    if (claims.isEmpty()) {
      LOG.info(
          "Skipping commit-job submit for table {} (lost soft-slot race on chain policies {})",
          tableId,
          policyIds);
      return;
    }

    Map<String, String> jobConf;
    try {
      jobConf =
          jobConfBuilder.resolveForSubmit(MaintenanceJobSpecFactory.jobConfForCommitChain(spec));
    } catch (RuntimeException e) {
      actionMetaService.deleteClaims(claims.get());
      LOG.warn(
          "Skipping commit-job submit for table {}: invalid task_data: {}", tableId, e.toString());
      return;
    }

    Optional<Long> jobId = jobSubmitManager.submitAndBind(spec.metalake(), jobConf, claims.get());
    if (jobId.isPresent()) {
      LOG.info(
          "Submitted commit-job for table {} as job {} (chain={}, policies={})",
          tableId,
          jobId.get(),
          spec.getTasks().stream().map(Task::getMaintenanceType).collect(Collectors.toList()),
          policyIds);
    }
  }
}
