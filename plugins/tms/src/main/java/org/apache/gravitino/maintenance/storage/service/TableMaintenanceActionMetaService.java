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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
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
   *   <li>CAS-{@code UPDATE created_at} on an expired unbound row ({@code finished_at = 0})
   *   <li>Otherwise {@code INSERT} with {@code finished_at = 0}; unique conflict means lost race
   * </ol>
   *
   * <p>{@code finished_at = 0} is the soft exclusive slot (like {@code deleted_at}); it stays until
   * {@link #markFinished}. {@code created_at} is the unbound claim lease start.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param nowMs current epoch millis
   * @param claimTimeoutMs unbound claim TTL
   * @return claim ({@code id} + acquire-time {@code created_at}) when acquired, empty when another
   *     live claim or open run holds the slot
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "tryAcquireTableMaintenanceAction")
  public Optional<OccupancyClaim> tryAcquireInFlight(
      long tableId, long policyId, long nowMs, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    long expireBefore = nowMs - claimTimeoutMs;
    int reclaimed =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.reclaimExpiredUnbound(tableId, policyId, nowMs, expireBefore));
    if (reclaimed == 1) {
      TableMaintenanceActionPO unbound =
          SessionUtils.getWithoutCommit(
              TableMaintenanceActionMapper.class,
              mapper -> mapper.selectUnboundByTableAndPolicy(tableId, policyId));
      Preconditions.checkState(unbound != null && unbound.id() != null, "reclaim lost unbound row");
      // Fence with the created_at this acquire wrote (nowMs), never re-read created_at.
      return Optional.of(new OccupancyClaim(unbound.id(), nowMs));
    }

    TableMaintenanceActionPO existing =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectUnboundByTableAndPolicy(tableId, policyId));
    if (existing != null
        && existing.createdAt() != null
        && nowMs - existing.createdAt() < claimTimeoutMs) {
      return Optional.empty();
    }

    TableMaintenanceActionPO actionMeta =
        TableMaintenanceActionPO.builder()
            .withTableId(tableId)
            .withPolicyId(policyId)
            .withCreatedAt(nowMs)
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
    Preconditions.checkState(actionMeta.id() != null, "insert did not return generated id");
    return Optional.of(new OccupancyClaim(actionMeta.id(), nowMs));
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
    Optional<OccupancyClaim> acquired =
        tryAcquireInFlight(tableId, policyId, System.currentTimeMillis(), claimTimeoutMs);
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
            mapper -> mapper.bindJobId(claim.id(), jobId, claim.createdAt()));
    Preconditions.checkState(
        updated == 1,
        "expected to bind job_id on exactly one in-flight action, updated=%s",
        updated);
  }

  /**
   * @param id action row id
   * @return action row or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionById")
  public TableMaintenanceActionPO getById(long id) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class, mapper -> mapper.selectById(id));
  }

  /**
   * @param jobId Gravitino job run id
   * @return action row or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableMaintenanceActionByJobId")
  public TableMaintenanceActionPO getByJobId(long jobId) {
    return SessionUtils.getWithoutCommit(
        TableMaintenanceActionMapper.class, mapper -> mapper.selectByJobId(jobId));
  }

  /**
   * Records the sampled Iceberg snapshot id ({@code S0}) once for a running action.
   *
   * @param jobId Gravitino job run id
   * @param beforeSnapshotId sampled snapshot id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "recordTableMaintenanceActionBeforeSnapshot")
  public void recordBeforeSnapshot(long jobId, long beforeSnapshotId) {
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.updateBeforeSnapshot(jobId, beforeSnapshotId));
    Preconditions.checkState(
        updated == 1,
        "expected to set before_snapshot_id once on an unfinished action, updated=%s",
        updated);
  }

  /**
   * Marks the action finished with the terminal snapshot id.
   *
   * <p>{@code UNIQUE(table_id, policy_id, finished_at)} exists so {@code finished_at = 0} is a
   * single soft slot per pair (like {@code deleted_at}). Only one in-flight row can finish that
   * pair at a time, so two completed rows cannot race to the same millisecond {@code finished_at}.
   *
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish timestamp millis
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "markTableMaintenanceActionFinished")
  public void markFinished(long jobId, Long afterSnapshotId, long finishedAt) {
    Preconditions.checkArgument(finishedAt > 0, "finishedAt must be positive");
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.markFinished(jobId, afterSnapshotId, finishedAt));
    Preconditions.checkState(
        updated == 1, "expected to finish exactly one in-flight action, updated=%s", updated);
  }

  /**
   * Deletes the {@code table_maintenance_action} row. Does not delete {@code
   * table_snapshot_metrics}; metrics are retained for TTL/N cleanup.
   *
   * @param id action row id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTableMaintenanceActionById")
  public void deleteById(long id) {
    SessionUtils.doWithCommit(TableMaintenanceActionMapper.class, mapper -> mapper.deleteById(id));
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
}
