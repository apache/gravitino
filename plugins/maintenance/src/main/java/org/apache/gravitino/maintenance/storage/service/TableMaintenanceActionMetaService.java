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
package org.apache.gravitino.maintenance.storage.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.google.common.base.Preconditions;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.occupancy.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceActionMapper;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** DAO service for {@code table_maintenance_action}. */
public class TableMaintenanceActionMetaService {

  private static final TableMaintenanceActionMetaService INSTANCE =
      new TableMaintenanceActionMetaService();

  private TableMaintenanceActionMetaService() {}

  /**
   * @return singleton service instance
   */
  public static TableMaintenanceActionMetaService getInstance() {
    return INSTANCE;
  }

  /**
   * Tries to acquire an unbound occupancy claim before {@code runJob}.
   *
   * <ol>
   *   <li>Abandon an expired unbound row by setting {@code finished_at} to the database clock (row
   *       is retained; soft slot freed)
   *   <li>{@code INSERT} a new row with {@code finished_at = 0} and DB {@code created_at}; unique
   *       conflict means lost race
   * </ol>
   *
   * <p>{@code finished_at = 0} is the soft exclusive slot (like {@code deleted_at}); it stays until
   * {@link #markFinished}. {@code created_at} is the unbound claim lease start from the database
   * clock so multi-node TTL does not depend on JVM skew. TTL abandon never reuses {@code
   * action_id}; Spark must be injected with the new id.
   *
   * <p><b>Submit contract:</b> {@link #bindJobId} only fences the DB bind. It cannot undo a {@code
   * runJob} that already succeeded. OccupancyGate / submit follow-ups must cancel orphaned Spark
   * work when bind fails so a crash between submit and bind cannot leave dual runners.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL
   * @return claim ({@code action_id} + DB {@code created_at}) when acquired, empty when another
   *     live claim or open run holds the slot
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "tryAcquireTableMaintenanceAction")
  public Optional<OccupancyClaim> tryAcquireInFlight(
      long tableId, long policyId, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    SessionUtils.doWithCommit(
        TableMaintenanceActionMapper.class,
        mapper -> mapper.reclaimExpiredUnbound(tableId, policyId, claimTimeoutMs));

    TableMaintenanceActionPO live =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectLiveUnboundByTableAndPolicy(tableId, policyId, claimTimeoutMs));
    if (live != null) {
      return Optional.empty();
    }

    TableMaintenanceActionPO actionMeta =
        TableMaintenanceActionPO.builder()
            .withTableId(tableId)
            .withPolicyId(policyId)
            .withFinishedAt(0L)
            .build();
    try {
      SessionUtils.doWithCommit(
          TableMaintenanceActionMapper.class, mapper -> mapper.insertAction(actionMeta));
    } catch (RuntimeException e) {
      if (isUniqueViolation(e)) {
        return Optional.empty();
      }
      throw e;
    }
    Preconditions.checkState(
        actionMeta.actionId() != null, "insert did not return generated action_id");
    TableMaintenanceActionPO inserted =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class, mapper -> mapper.selectById(actionMeta.actionId()));
    Preconditions.checkState(
        inserted != null && inserted.createdAt() != null, "insert lost created_at");
    return Optional.of(new OccupancyClaim(inserted.actionId(), inserted.createdAt()));
  }

  /**
   * Tries to acquire unbound soft slots for every entry in {@code policyIds} in one transaction
   * (commit-chain submit).
   *
   * <p>Expired unbound rows are abandoned ({@code finished_at = now}) first; every policy is then
   * inserted with a single multi-row {@code INSERT} (new {@code action_id}s). Any live slot or
   * unique conflict aborts the transaction so the caller never holds a partial chain.
   *
   * @param tableId table entity id
   * @param policyIds chain member policy ids (duplicates collapsed, order preserved)
   * @param claimTimeoutMs unbound claim TTL
   * @return one {@link OccupancyClaim} per distinct policy id, or empty when the chain cannot be
   *     claimed atomically
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "tryAcquireTableMaintenanceActionBatch")
  public Optional<List<OccupancyClaim>> tryAcquireInFlightBatch(
      long tableId, List<Long> policyIds, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    Preconditions.checkArgument(
        policyIds != null && !policyIds.isEmpty(), "policyIds must not be empty");
    List<Long> distinct = new ArrayList<>(new LinkedHashSet<>(policyIds));
    try {
      return Optional.of(
          SessionUtils.doWithCommitAndFetchResult(
              TableMaintenanceActionMapper.class,
              mapper -> acquireBatchInTransaction(mapper, tableId, distinct, claimTimeoutMs)));
    } catch (SoftSlotBusyException e) {
      return Optional.empty();
    } catch (RuntimeException e) {
      if (isUniqueViolation(e)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  private static List<OccupancyClaim> acquireBatchInTransaction(
      TableMaintenanceActionMapper mapper,
      long tableId,
      List<Long> policyIds,
      long claimTimeoutMs) {
    for (long policyId : policyIds) {
      mapper.reclaimExpiredUnbound(tableId, policyId, claimTimeoutMs);
      TableMaintenanceActionPO live =
          mapper.selectLiveUnboundByTableAndPolicy(tableId, policyId, claimTimeoutMs);
      if (live != null) {
        throw new SoftSlotBusyException();
      }
    }
    mapper.insertActions(tableId, policyIds);
    List<OccupancyClaim> claims = new ArrayList<>(policyIds.size());
    for (long policyId : policyIds) {
      TableMaintenanceActionPO unbound = mapper.selectUnboundByTableAndPolicy(tableId, policyId);
      Preconditions.checkState(
          unbound != null && unbound.actionId() != null && unbound.createdAt() != null,
          "batch acquire lost unbound row for table %s policy %s",
          tableId,
          policyId);
      claims.add(new OccupancyClaim(unbound.actionId(), unbound.createdAt()));
    }
    return claims;
  }

  /**
   * Acquires an unbound claim using {@link TableMaintenanceConfig#claimTimeoutMs()} or fails.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param config maintenance config supplying the unbound claim TTL
   * @return acquired claim
   */
  public OccupancyClaim insertInFlight(long tableId, long policyId, TableMaintenanceConfig config) {
    Preconditions.checkNotNull(config, "config must not be null");
    return insertInFlight(tableId, policyId, config.claimTimeoutMs());
  }

  /**
   * Acquires an unbound claim with the given claim timeout or fails.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL
   * @return acquired claim
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertTableMaintenanceAction")
  public OccupancyClaim insertInFlight(long tableId, long policyId, long claimTimeoutMs) {
    Optional<OccupancyClaim> acquired = tryAcquireInFlight(tableId, policyId, claimTimeoutMs);
    Preconditions.checkState(acquired.isPresent(), "failed to acquire unbound occupancy claim");
    return acquired.get();
  }

  /**
   * Binds {@code job_run_meta.job_run_id} after a successful {@code runJob}. Does not release the
   * soft slot ({@code finished_at} stays {@code 0} until {@link #markFinished}).
   *
   * <p>Pass the {@link OccupancyClaim} from acquire / reclaim as-is. Do not re-read {@code
   * created_at} from the row: after another node reclaims, that would observe the new fence and
   * defeat reclaim fencing. The CAS matches zero rows for a stale claim.
   *
   * <p>Idempotent when the row is already bound to the same {@code jobId} and still in-flight
   * (Spark may lazy-bind before submit-side bind). Still fails when the claim was reclaimed or
   * bound to a different job.
   *
   * <p>This CAS does <em>not</em> prevent duplicate Spark submissions if {@code runJob} already
   * ran; see {@link #tryAcquireInFlight} submit contract.
   *
   * @param claim claim returned by {@link #tryAcquireInFlight} / {@link #insertInFlight}
   * @param jobId Gravitino job run id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "bindTableMaintenanceActionJobId")
  public void bindJobId(OccupancyClaim claim, long jobId) {
    Preconditions.checkNotNull(claim, "claim must not be null");
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.bindJobId(claim.actionId(), jobId, claim.createdAt()));
    if (updated == 1) {
      return;
    }
    TableMaintenanceActionPO current = getById(claim.actionId());
    if (current != null
        && Objects.equals(current.jobId(), jobId)
        && current.finishedAt() != null
        && current.finishedAt() == 0L) {
      return;
    }
    throw new IllegalStateException(
        String.format(
            "expected to bind job_id on exactly one in-flight action, updated=%s", updated));
  }

  /**
   * Lazy-binds {@code jobId} onto the unbound in-flight claim for {@code (tableId, policyId)}.
   *
   * <p><b>Not for Spark callbacks.</b> Callbacks must address the soft slot by the injected {@code
   * action_id} ({@link #getById} / metrics APIs). Ownership must not be inferred from {@code
   * (table_id, policy_id)} after TTL reclaim (old row finished, new {@code action_id}). Kept for
   * tests and rare control-plane repair; submit-side {@link #bindJobId} remains the normal path.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param jobId Gravitino job run id
   * @return {@code true} when a row is now bound to {@code jobId}
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "bindTableMaintenanceActionJobIdIfUnbound")
  public boolean bindJobIdIfUnbound(long tableId, long policyId, long jobId) {
    TableMaintenanceActionPO already = getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
    if (already != null && already.finishedAt() != null && already.finishedAt() == 0L) {
      return true;
    }
    TableMaintenanceActionPO unbound =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectUnboundByTableAndPolicy(tableId, policyId));
    if (unbound == null || unbound.actionId() == null || unbound.createdAt() == null) {
      return false;
    }
    try {
      bindJobId(new OccupancyClaim(unbound.actionId(), unbound.createdAt()), jobId);
      return true;
    } catch (IllegalStateException e) {
      TableMaintenanceActionPO bound = getByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId);
      return bound != null && bound.finishedAt() != null && bound.finishedAt() == 0L;
    }
  }

  /**
   * Binds the same Gravitino job run id to every claim from {@link #tryAcquireInFlightBatch} in one
   * transaction (commit-chain submit).
   *
   * <p>Idempotent per claim when the row is already bound to {@code jobId} and still in-flight
   * (Spark may lazy-bind before submit-side bind), matching {@link #bindJobId}.
   *
   * @param claims claims returned by batch acquire
   * @param jobId Gravitino job run id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "bindTableMaintenanceActionJobIds")
  public void bindJobIds(List<OccupancyClaim> claims, long jobId) {
    Preconditions.checkArgument(claims != null && !claims.isEmpty(), "claims must not be empty");
    SessionUtils.doWithCommit(
        TableMaintenanceActionMapper.class,
        mapper -> {
          for (OccupancyClaim claim : claims) {
            Preconditions.checkNotNull(claim, "claim must not be null");
            int updated = mapper.bindJobId(claim.actionId(), jobId, claim.createdAt());
            if (updated == 1) {
              continue;
            }
            TableMaintenanceActionPO current = mapper.selectById(claim.actionId());
            Preconditions.checkState(
                current != null
                    && Objects.equals(current.jobId(), jobId)
                    && current.finishedAt() != null
                    && current.finishedAt() == 0L,
                "expected to bind job_id on claim id=%s, updated=%s",
                claim.actionId(),
                updated);
          }
        });
  }

  /**
   * Releases unbound claims after a failed submit. Each delete is fenced on {@code (action_id,
   * created_at)} so a concurrent abandon's new soft slot is not removed.
   *
   * @param claims claims to release
   */
  public void deleteClaims(List<OccupancyClaim> claims) {
    if (claims == null || claims.isEmpty()) {
      return;
    }
    for (OccupancyClaim claim : claims) {
      if (claim == null) {
        continue;
      }
      deleteUnboundClaim(claim);
    }
  }

  /**
   * @param actionId action row id
   * @return action row or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionById")
  public TableMaintenanceActionPO getById(long actionId) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class, mapper -> mapper.selectById(actionId));
  }

  /**
   * Lists all action rows bound to a Gravitino job run. One Spark job may cover multiple tables, so
   * the same {@code job_id} can appear on more than one row.
   *
   * @param jobId Gravitino job run id
   * @return bound action rows (empty when none)
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listTableMaintenanceActionsByJobId")
  public List<TableMaintenanceActionPO> listByJobId(long jobId) {
    List<TableMaintenanceActionPO> rows =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class, mapper -> mapper.selectByJobId(jobId));
    return rows == null ? Collections.emptyList() : rows;
  }

  /**
   * @param jobId Gravitino job run id
   * @param tableId table entity id
   * @return action row for that job/table or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionByJobIdAndTableId")
  public TableMaintenanceActionPO getByJobIdAndTableId(long jobId, long tableId) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class,
        mapper -> mapper.selectByJobIdAndTableId(jobId, tableId));
  }

  /**
   * @param jobId Gravitino job run id
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return action row for that job/table/policy or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionByJobIdAndTableIdAndPolicyId")
  public TableMaintenanceActionPO getByJobIdAndTableIdAndPolicyId(
      long jobId, long tableId, long policyId) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class,
        mapper -> mapper.selectByJobIdAndTableIdAndPolicyId(jobId, tableId, policyId));
  }

  /**
   * Convenience for single-table jobs. Prefer {@link #getByJobIdAndTableIdAndPolicyId}, {@link
   * #getByJobIdAndTableId}, or {@link #listByJobId} when a job may cover multiple tables or
   * policies.
   *
   * @param jobId Gravitino job run id
   * @return the sole action row, or null when none
   * @throws IllegalStateException when more than one table is bound to {@code jobId}
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionByJobId")
  public TableMaintenanceActionPO getByJobId(long jobId) {
    List<TableMaintenanceActionPO> rows = listByJobId(jobId);
    if (rows.isEmpty()) {
      return null;
    }
    Preconditions.checkState(
        rows.size() == 1,
        "multiple maintenance actions for job_id=%s; use getByJobIdAndTableIdAndPolicyId or listByJobId",
        jobId);
    return rows.get(0);
  }

  /**
   * Records the S0 metrics row id for a bound in-flight action.
   *
   * <p>Requires {@code job_id IS NOT NULL} and {@code finished_at = 0}. Safe to retry with the same
   * {@code beforeMetricsId}. A different metrics id on an already-set row fails.
   *
   * @param actionId action row id
   * @param beforeMetricsId {@code table_snapshot_metrics.metrics_id} for S0
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "recordTableMaintenanceActionBeforeMetrics")
  public void recordBeforeMetrics(long actionId, long beforeMetricsId) {
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.updateBeforeMetrics(actionId, beforeMetricsId));
    if (updated == 1) {
      return;
    }
    TableMaintenanceActionPO current = getById(actionId);
    Preconditions.checkState(
        current != null
            && current.jobId() != null
            && current.finishedAt() != null
            && current.finishedAt() == 0L,
        "expected a bound unfinished action for action_id=%s",
        actionId);
    Preconditions.checkState(
        current.beforeMetricsId() != null && current.beforeMetricsId() == beforeMetricsId,
        "before_metrics_id already set to %s, refusing %s",
        current.beforeMetricsId(),
        beforeMetricsId);
  }

  /**
   * Records the S1 metrics row id for a bound in-flight action.
   *
   * <p>Safe to retry with the same {@code afterMetricsId}. A different metrics id on an already-set
   * row fails.
   *
   * @param actionId action row id
   * @param afterMetricsId {@code table_snapshot_metrics.metrics_id} for S1
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "recordTableMaintenanceActionAfterMetrics")
  public void recordAfterMetrics(long actionId, long afterMetricsId) {
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.updateAfterMetrics(actionId, afterMetricsId));
    if (updated == 1) {
      return;
    }
    TableMaintenanceActionPO current = getById(actionId);
    Preconditions.checkState(
        current != null
            && current.jobId() != null
            && current.finishedAt() != null
            && current.finishedAt() == 0L,
        "expected a bound unfinished action for action_id=%s",
        actionId);
    Preconditions.checkState(
        current.afterMetricsId() != null && current.afterMetricsId() == afterMetricsId,
        "after_metrics_id already set to %s, refusing %s",
        current.afterMetricsId(),
        afterMetricsId);
  }

  /**
   * Marks one bound in-flight action finished by {@code action_id}.
   *
   * <p>{@code UNIQUE(table_id, policy_id, finished_at)} exists so {@code finished_at = 0} is a
   * single soft slot per pair (like {@code deleted_at}). Only one in-flight row can finish that
   * pair at a time, so two completed rows cannot race to the same millisecond {@code finished_at}.
   *
   * <p>Does not write metrics pointers; call {@link #recordBeforeMetrics} / {@link
   * #recordAfterMetrics} separately when sampling. Safe to retry when the row is already finished.
   *
   * @param actionId action row id
   * @param finishedAt finish timestamp millis
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "markTableMaintenanceActionFinished")
  public void markFinished(long actionId, long finishedAt) {
    Preconditions.checkArgument(finishedAt > 0, "finishedAt must be positive");
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.markFinished(actionId, finishedAt));
    if (updated == 1) {
      return;
    }
    TableMaintenanceActionPO current = getById(actionId);
    Preconditions.checkState(
        current != null && current.finishedAt() != null && current.finishedAt() > 0L,
        "expected finished action for action_id=%s, updated=%s",
        actionId,
        updated);
  }

  /**
   * Deletes the {@code table_maintenance_action} row. Does not delete {@code
   * table_snapshot_metrics}; metrics are retained for TTL/N cleanup.
   *
   * @param actionId action row id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTableMaintenanceActionById")
  public void deleteById(long actionId) {
    SessionUtils.doWithCommit(
        TableMaintenanceActionMapper.class, mapper -> mapper.deleteById(actionId));
  }

  /**
   * Releases an unbound occupancy claim when submit / bind fails.
   *
   * <p>Deletes only when {@code (action_id, created_at)} still match and the row is unbound. A
   * concurrent abandon that closed this row leaves the successor soft slot intact (returns {@code
   * false}).
   *
   * @param claim claim returned by {@link #tryAcquireInFlight} / {@link #insertInFlight}
   * @return {@code true} when this claim lease was deleted
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteUnboundTableMaintenanceActionClaim")
  public boolean deleteUnboundClaim(OccupancyClaim claim) {
    Preconditions.checkNotNull(claim, "claim must not be null");
    int deleted =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.deleteUnboundClaim(claim.actionId(), claim.createdAt()));
    return deleted == 1;
  }

  /**
   * Deletes the {@code table_maintenance_action} row keyed by job run id. Does not delete metrics.
   *
   * @param jobId Gravitino job run id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTableMaintenanceActionByJobId")
  public void deleteByJobId(long jobId) {
    SessionUtils.doWithCommit(
        TableMaintenanceActionMapper.class, mapper -> mapper.deleteByJobId(jobId));
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return open rows with {@code finished_at = 0} (at most one per table/policy; may include a
   *     terminal job not yet marked finished)
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listInFlightTableMaintenanceActions")
  public List<TableMaintenanceActionPO> listInFlight(long tableId, long policyId) {
    List<TableMaintenanceActionPO> open =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectInFlightByTableAndPolicy(tableId, policyId));
    return open == null ? Collections.emptyList() : open;
  }

  /**
   * Whether an unbound claim is still within TTL on the <strong>database</strong> clock.
   *
   * <p>Must not be evaluated with {@code System.currentTimeMillis()} against {@code created_at}:
   * acquire/reclaim already use DB time, and JVM skew would disagree with that lease.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL
   * @return true when a live unbound claim holds the soft slot
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "hasLiveUnboundTableMaintenanceAction")
  public boolean hasLiveUnboundClaim(long tableId, long policyId, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    TableMaintenanceActionPO live =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectLiveUnboundByTableAndPolicy(tableId, policyId, claimTimeoutMs));
    return live != null;
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return latest {@code finished_at}, or null when none finished yet
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getMaxFinishedAtTableMaintenanceAction")
  public Long maxFinishedAt(long tableId, long policyId) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class,
        mapper -> mapper.selectMaxFinishedAt(tableId, policyId));
  }

  private static boolean isUniqueViolation(Throwable throwable) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t instanceof SQLIntegrityConstraintViolationException) {
        return true;
      }
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("unique") || lower.contains("duplicate")) {
          return true;
        }
      }
    }
    return false;
  }

  /** Thrown inside a batch-acquire transaction to roll back when any soft slot is busy. */
  private static final class SoftSlotBusyException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }
}
