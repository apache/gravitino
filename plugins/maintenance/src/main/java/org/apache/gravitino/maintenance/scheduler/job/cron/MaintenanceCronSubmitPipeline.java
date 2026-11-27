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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.MaintenanceActionManager;
import org.apache.gravitino.maintenance.MaintenanceJobManager;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cron-pool control-plane submit (§5.2.2): consume schedule-time {@link MaintenanceJobSpec} →
 * occupancy gate → soft-slot claim for every task → one {@link MaintenanceJobManager#submitAndBind}
 * Spark job (single-table or snapshot-expiry batch).
 *
 * <p>Single-table {@code table:...} and snapshot-expiry {@code batch:...} rows share the same spec
 * shape. After merge, a {@code table:} instance may still hold multiple tasks (members live in
 * {@code task_data}); occupancy uses each task's own {@code policyId}. If any task is occupied, or
 * any soft-slot claim fails, the whole pick stops without partial submits (one-shot row is still
 * removed by the caller).
 *
 * <p>Does not sample metrics or evaluate trigger-expr (both run inside Spark from per-task {@code
 * rules}).
 */
public final class MaintenanceCronSubmitPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceCronSubmitPipeline.class);

  private final MaintenanceActionManager actionManager;
  private final TableMaintenanceActionMetaService actionMetaService;
  private final MaintenanceJobManager jobManager;
  private final MaintenanceJobConfBuilder jobConfBuilder;
  private final long claimTimeoutMs;

  /**
   * @param actionManager occupancy checks (expand/cron does not apply {@code minIntervalMs})
   * @param actionMetaService soft-slot claim
   * @param jobManager runJob + bind (and orphan cancel)
   * @param jobConfBuilder resolves schedule-time conf-key refs before submit
   * @param claimTimeoutMs unbound claim TTL
   */
  public MaintenanceCronSubmitPipeline(
      MaintenanceActionManager actionManager,
      TableMaintenanceActionMetaService actionMetaService,
      MaintenanceJobManager jobManager,
      MaintenanceJobConfBuilder jobConfBuilder,
      long claimTimeoutMs) {
    this.actionManager = Preconditions.checkNotNull(actionManager);
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.jobManager = Preconditions.checkNotNull(jobManager);
    this.jobConfBuilder = Preconditions.checkNotNull(jobConfBuilder);
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * Runs one cron-job pick for {@code instanceId}.
   *
   * @param instanceId db-scheduler cron-job instance id
   * @param spec schedule-time payload; required
   */
  public void submit(String instanceId, @Nullable MaintenanceJobSpec spec) {
    if (spec == null || !spec.isValid()) {
      LOG.warn("Skipping cron-job submit for {} with missing/invalid task_data spec", instanceId);
      return;
    }

    Optional<long[]> tableInstance = MaintenanceTaskIds.parseCronTable(instanceId);
    Optional<long[]> batchInstance = MaintenanceTaskIds.parseCronBatch(instanceId);
    if (tableInstance.isEmpty() && batchInstance.isEmpty()) {
      LOG.warn("Skipping cron-job submit for unparseable instance id {}", instanceId);
      return;
    }

    // Instance id is row identity; member tables live in task_data (a table: row may hold peers
    // after merge). Soft slot stays per task.policyId.
    if (tableInstance.isPresent()) {
      long instanceTableId = tableInstance.get()[0];
      long instancePolicyId = tableInstance.get()[1];
      Optional<Task> instanceTask =
          spec.getTasks().stream()
              .filter(t -> t.getTableId() == instanceTableId && t.getPolicyId() == instancePolicyId)
              .findFirst();
      if (instanceTask.isEmpty()) {
        LOG.warn(
            "Skipping cron-job submit for {}: instance tableId {} policyId {} missing from spec"
                + " tasks {}",
            instanceId,
            instanceTableId,
            instancePolicyId,
            spec.tableIds());
        return;
      }
    }
    if (batchInstance.isPresent()) {
      // Batch rows name one policy id; allow mixed-policy merged specs as long as that policy
      // appears on at least one task (same as schedule-time batch keying).
      long instancePolicyId = batchInstance.get()[1];
      boolean policyPresent =
          spec.getTasks().stream().anyMatch(t -> t.getPolicyId() == instancePolicyId);
      if (!policyPresent) {
        LOG.warn(
            "Skipping cron-job submit for {}: instance policyId {} missing from spec tasks {}",
            instanceId,
            instancePolicyId,
            spec.tableIds());
        return;
      }
    }

    // Expand/cron does not apply minIntervalMs (commitTrigger only). Pick blocks on occupied soft
    // slots; any occupied member aborts the whole pick so a batch row is not partially submitted
    // then deleted.
    for (Task task : spec.getTasks()) {
      if (actionManager.isOccupied(task.getTableId(), task.getPolicyId())) {
        LOG.info(
            "Skipping cron-job {} entirely (table {} occupied for policy {})",
            instanceId,
            task.getTableId(),
            task.getPolicyId());
        return;
      }
    }

    List<OccupancyClaim> claims = new ArrayList<>(spec.getTasks().size());
    for (Task task : spec.getTasks()) {
      Optional<OccupancyClaim> claim =
          actionMetaService.tryAcquireInFlight(
              task.getTableId(), task.getPolicyId(), claimTimeoutMs);
      if (claim.isEmpty()) {
        if (!claims.isEmpty()) {
          actionMetaService.deleteClaims(claims);
        }
        LOG.info(
            "Skipping cron-job {} entirely (lost soft-slot race on table {} policy {})",
            instanceId,
            task.getTableId(),
            task.getPolicyId());
        return;
      }
      claims.add(claim.get());
    }

    Map<String, String> jobConf;
    try {
      jobConf = jobConfBuilder.resolveForSubmit(MaintenanceJobSpecFactory.jobConfForSpec(spec));
    } catch (RuntimeException e) {
      actionMetaService.deleteClaims(claims);
      LOG.warn("Skipping cron-job submit for {}: invalid task_data: {}", instanceId, e.toString());
      return;
    }

    Optional<Long> jobId = jobManager.submitAndBind(spec.metalake(), jobConf, claims);
    if (jobId.isPresent()) {
      LOG.info(
          "Submitted cron-job {} as job {} (tables={}, policies={})",
          instanceId,
          jobId.get(),
          spec.tableIds(),
          spec.getTasks().stream().map(Task::getPolicyId).collect(Collectors.toList()));
    }
  }
}
