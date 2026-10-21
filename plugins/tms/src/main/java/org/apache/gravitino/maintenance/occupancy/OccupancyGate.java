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
import org.apache.gravitino.storage.relational.service.JobMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expand / submit occupancy checks against {@code table_maintenance_action} (§5.2).
 *
 * <p>Unbound rows ({@code job_id} null, {@code finished_at = 0}) are a short claim: occupied while
 * still within TTL on the <strong>database</strong> clock (same timeline as acquire/reclaim). Bound
 * rows consult {@link JobMetaService}; when that job is already terminal or confirmed missing,
 * {@link #reclaimTerminalBound} marks {@code finished_at} so the soft slot can be claimed again
 * (without inventing stub S1 metrics rows or copying {@code before_snapshot_id} into {@code
 * after_snapshot_id}). A later finish listener may still backfill a real S1 via {@link
 * TableMaintenanceActionMetaService#markFinished}.
 */
public final class OccupancyGate {

  private static final Logger LOG = LoggerFactory.getLogger(OccupancyGate.class);

  private static final Set<JobHandle.Status> ACTIVE =
      EnumSet.of(JobHandle.Status.QUEUED, JobHandle.Status.STARTED, JobHandle.Status.CANCELLING);

  private static final JobMetaService JOB_SERVICE = JobMetaService.getInstance();
  private static final TableMetaService TABLE_SERVICE = TableMetaService.getInstance();

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
    // After reclaim, any remaining bound open row occupies the soft slot (ACTIVE job, unresolved
    // lookup, or a terminal/missing job whose markFinished raced/failed). Fail closed so
    // UNIQUE(table, policy, finished_at=0) cannot be ignored while still blocking
    // tryAcquireInFlight.
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Job-terminal reconciler failsafe (§5.2.2): when Spark finish callback was missed, mark the
   * action finished so the soft slot can be reused. Does not invent stub metrics rows (those would
   * pollute {@code table_snapshot_metrics}), and does not copy {@code before_snapshot_id} into
   * {@code after_snapshot_id} — only a real S1 (or null) is stored (§5.2.3).
   *
   * <p>Only reclaim when the bound job is confirmed terminal or confirmed missing. An unresolved
   * job lookup (for example the table/metalake cannot be resolved) leaves the row open.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   */
  public void reclaimTerminalBound(long tableId, long policyId) {
    for (TableMaintenanceActionPO open : actionMetaService.listInFlight(tableId, policyId)) {
      if (open.jobId() == null) {
        continue;
      }
      BoundJobLookup lookup = lookupBoundJob(open.jobId(), tableId);
      if (lookup.kind == BoundJobLookup.Kind.ACTIVE
          || lookup.kind == BoundJobLookup.Kind.UNRESOLVED) {
        if (lookup.kind == BoundJobLookup.Kind.UNRESOLVED) {
          LOG.warn(
              "Skipping reclaim of job {} for table {} policy {}: metalake could not be resolved",
              open.jobId(),
              tableId,
              policyId);
        }
        continue;
      }
      JobEntity job = lookup.job;
      long finishedAt =
          job != null && job.finishedAt() != null && job.finishedAt() > 0
              ? job.finishedAt()
              : System.currentTimeMillis();
      // Keep a real after snapshot only; never invent S1 from S0.
      Long afterSnapshotId = open.afterSnapshotId();
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
      BoundJobLookup lookup = lookupBoundJob(open.jobId(), tableId);
      if (lookup.kind != BoundJobLookup.Kind.TERMINAL || lookup.job == null) {
        continue;
      }
      Long fromJob =
          lookup.job.finishedAt() != null && lookup.job.finishedAt() > 0
              ? lookup.job.finishedAt()
              : null;
      if (fromJob == null) {
        continue;
      }
      if (latest == null || fromJob > latest) {
        latest = fromJob;
      }
    }
    return latest;
  }

  /**
   * Resolves the bound job through the table's metalake, not the current policy. A deleted policy
   * must not make a still-running job look "missing".
   */
  private static BoundJobLookup lookupBoundJob(long jobId, long tableId) {
    String metalake = TABLE_SERVICE.getMetalakeNameByTableId(tableId);
    if (metalake == null) {
      return BoundJobLookup.unresolved();
    }
    try {
      JobEntity job =
          JOB_SERVICE.getJobByIdentifier(
              NameIdentifierUtil.ofJob(metalake, JobHandle.JOB_ID_PREFIX + jobId));
      if (ACTIVE.contains(job.status())) {
        return BoundJobLookup.active(job);
      }
      return BoundJobLookup.terminal(job);
    } catch (NoSuchEntityException e) {
      return BoundJobLookup.missing();
    }
  }

  /** Outcome of resolving a bound Gravitino job for reclaim / cooldown. */
  private static final class BoundJobLookup {
    enum Kind {
      /** Job meta loaded and still queued/started/cancelling. */
      ACTIVE,
      /** Job meta loaded and already terminal. */
      TERMINAL,
      /** Metalake resolved but job meta is gone. */
      MISSING,
      /** Table/metalake could not be resolved; do not treat as terminal. */
      UNRESOLVED
    }

    final Kind kind;
    @Nullable final JobEntity job;

    private BoundJobLookup(Kind kind, @Nullable JobEntity job) {
      this.kind = kind;
      this.job = job;
    }

    static BoundJobLookup active(JobEntity job) {
      return new BoundJobLookup(Kind.ACTIVE, job);
    }

    static BoundJobLookup terminal(JobEntity job) {
      return new BoundJobLookup(Kind.TERMINAL, job);
    }

    static BoundJobLookup missing() {
      return new BoundJobLookup(Kind.MISSING, null);
    }

    static BoundJobLookup unresolved() {
      return new BoundJobLookup(Kind.UNRESOLVED, null);
    }
  }
}
