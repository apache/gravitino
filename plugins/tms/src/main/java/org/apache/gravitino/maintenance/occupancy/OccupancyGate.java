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
package org.apache.gravitino.maintenance.occupancy;

import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.JobMetaService;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_action} (§5.2).
 *
 * <p>Unbound rows ({@code job_id} null, {@code finished_at = 0}) are a short claim: occupied while
 * still within TTL on the <strong>database</strong> clock (same timeline as acquire/reclaim). Bound
 * rows consult {@link JobMetaService}; when that job is already terminal, {@link
 * #reclaimTerminalBound} marks {@code finished_at} so the soft slot can be claimed again (without
 * inventing stub S1 metrics rows).
 */
public final class OccupancyGate {

  private static final Logger LOG = LoggerFactory.getLogger(OccupancyGate.class);

  private static final Set<JobHandle.Status> ACTIVE =
      EnumSet.of(JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING);

  private final TableMaintenanceActionMetaService actionMetaService;
  private final long claimTimeoutMs;

  /**
   * @param actionMetaService maintenance action DAO
   * @param claimTimeoutMs unbound claim TTL
   */
  public OccupancyGate(TableMaintenanceActionMetaService actionMetaService, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    this.actionMetaService = Preconditions.checkNotNull(actionMetaService);
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * @param actionMetaService maintenance action DAO
   */
  public OccupancyGate(TableMaintenanceActionMetaService actionMetaService) {
    this(actionMetaService, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
  }

  /** Uses {@link TableMaintenanceActionMetaService#getInstance()} and default claim timeout. */
  public OccupancyGate() {
    this(TableMaintenanceActionMetaService.getInstance());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when any open row is a live unbound claim or a still-bound soft-slot row
   */
  public boolean isInFlight(long tableId, long policyId) {
    reclaimTerminalBound(tableId, policyId);
    if (actionMetaService.hasLiveUnboundClaim(tableId, policyId, claimTimeoutMs)) {
      return true;
    }
    // After reclaim, any remaining bound open row occupies the soft slot (ACTIVE job, or a
    // terminal/missing job whose markFinished raced/failed). Fail closed so UNIQUE(table, policy,
    // finished_at=0) cannot be ignored while still blocking tryAcquireInFlight.
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Job-terminal reconciler failsafe (§5.2.2): when Spark finish callback was missed, mark the
   * action finished using the best-known snapshot id. Does not invent stub metrics rows (those
   * would pollute {@code table_snapshot_metrics}).
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   */
  public void reclaimTerminalBound(long tableId, long policyId) {
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() == null) {
        continue;
      }
      JobEntity job = loadJob(open.jobId(), policyId);
      // Missing job meta (purged / unresolvable) must free the soft slot; otherwise UNIQUE(table,
      // policy, finished_at=0) blocks every future acquire.
      if (job != null && ACTIVE.contains(job.status())) {
        continue;
      }
      long finishedAt =
          job != null && job.finishedAt() != null && job.finishedAt() > 0
              ? job.finishedAt()
              : System.currentTimeMillis();
      Long afterSnapshotId =
          open.afterSnapshotId() != null ? open.afterSnapshotId() : open.beforeSnapshotId();
      try {
        actionMetaService.markFinished(open.jobId(), afterSnapshotId, finishedAt);
      } catch (RuntimeException e) {
        // Another node may have finished the row, or DB is unhealthy — log and keep fail-closed
        // via isInFlight seeing the still-open bound row.
        LOG.warn(
            "Failed to reclaim terminal/missing job {} for table {} policy {}: {}",
            open.jobId(),
            tableId,
            policyId,
            e.toString());
      }
    }
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis used only for cooldown (not unbound TTL)
   * @return true when the last known finish is still inside the cooldown window
   */
  public boolean withinMinInterval(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (minIntervalMs <= 0) {
      return false;
    }
    Long lastFinished = lastFinishedAt(tableId, policyId);
    return lastFinished != null && nowMs - lastFinished < minIntervalMs;
  }

  /**
   * Expand / submit should skip when occupied or still cooling down.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved minimum interval
   * @param nowMs current epoch millis used only for cooldown (not unbound TTL)
   * @return true when enqueue / submit must be skipped
   */
  public boolean shouldSkip(long tableId, long policyId, long minIntervalMs, long nowMs) {
    return isInFlight(tableId, policyId)
        || withinMinInterval(tableId, policyId, minIntervalMs, nowMs);
  }

  /**
   * Prefer {@code table_maintenance_action.finished_at}; if that column is still {@code 0} but a
   * Gravitino job is already terminal, use {@link JobEntity#finishedAt()} without writing back.
   */
  @Nullable
  private Long lastFinishedAt(long tableId, long policyId) {
    Long latest = actionMetaService.maxFinishedAt(tableId, policyId);
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() == null) {
        continue;
      }
      JobEntity job = loadJob(open.jobId(), policyId);
      if (job == null || ACTIVE.contains(job.status())) {
        continue;
      }
      Long fromJob = job.finishedAt() != null && job.finishedAt() > 0 ? job.finishedAt() : null;
      if (fromJob == null) {
        continue;
      }
      if (latest == null || fromJob > latest) {
        latest = fromJob;
      }
    }
    return latest;
  }

  @Nullable
  private static JobEntity loadJob(long jobId, long policyId) {
    PolicyEntity policy = PolicyMetaService.getInstance().getPolicyById(policyId);
    String metalake = policy == null ? null : policy.namespace().level(0);
    if (metalake == null) {
      return null;
    }
    try {
      return JobMetaService.getInstance()
          .getJobByIdentifier(NameIdentifierUtil.ofJob(metalake, JobHandle.JOB_ID_PREFIX + jobId));
    } catch (NoSuchEntityException e) {
      return null;
    }
  }
}
