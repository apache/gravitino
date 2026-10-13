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
import java.util.OptionalLong;
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
   *   <li>CAS-{@code UPDATE lock_until} on an expired unbound row ({@code claim_token = 1})
   *   <li>Otherwise {@code INSERT} with {@code claim_token = 1}; unique conflict means lost race
   * </ol>
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param nowMs current epoch millis
   * @param claimTimeoutMs unbound claim TTL
   * @return action id when acquired, empty when another live claim holds the slot
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "tryAcquireTableMaintenanceAction")
  public OptionalLong tryAcquireInFlight(
      long tableId, long policyId, long nowMs, long claimTimeoutMs) {
    Preconditions.checkArgument(claimTimeoutMs > 0, "claimTimeoutMs must be positive");
    long newLockUntil = nowMs + claimTimeoutMs;
    int reclaimed =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.reclaimExpiredUnbound(tableId, policyId, newLockUntil, nowMs));
    if (reclaimed == 1) {
      TableMaintenanceActionPO unbound =
          SessionUtils.getWithoutCommit(
              TableMaintenanceActionMapper.class,
              mapper -> mapper.selectUnboundByTableAndPolicy(tableId, policyId));
      Preconditions.checkState(unbound != null && unbound.id() != null, "reclaim lost unbound row");
      return OptionalLong.of(unbound.id());
    }

    TableMaintenanceActionPO existing =
        SessionUtils.getWithoutCommit(
            TableMaintenanceActionMapper.class,
            mapper -> mapper.selectUnboundByTableAndPolicy(tableId, policyId));
    if (existing != null && existing.lockUntil() != null && existing.lockUntil() > nowMs) {
      return OptionalLong.empty();
    }

    TableMaintenanceActionPO actionMeta =
        TableMaintenanceActionPO.builder()
            .withTableId(tableId)
            .withPolicyId(policyId)
            .withCreatedAt(nowMs)
            .withLockUntil(newLockUntil)
            .build();
    try {
      SessionUtils.doWithCommit(
          TableMaintenanceActionMapper.class, mapper -> mapper.insertAction(actionMeta));
    } catch (RuntimeException e) {
      if (isUniqueViolation(e)) {
        return OptionalLong.empty();
      }
      throw e;
    }
    Preconditions.checkState(actionMeta.id() != null, "insert did not return generated id");
    return OptionalLong.of(actionMeta.id());
  }

  /**
   * Acquires an unbound claim with the default claim timeout or fails.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return generated action id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertTableMaintenanceAction")
  public long insertInFlight(long tableId, long policyId) {
    OptionalLong acquired =
        tryAcquireInFlight(
            tableId,
            policyId,
            System.currentTimeMillis(),
            TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    Preconditions.checkState(acquired.isPresent(), "failed to acquire unbound occupancy claim");
    return acquired.getAsLong();
  }

  /**
   * Binds {@code job_run_meta.job_run_id} after a successful {@code runJob} and releases the
   * unbound claim slot.
   *
   * @param id action row id
   * @param jobId Gravitino job run id
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "bindTableMaintenanceActionJobId")
  public void bindJobId(long id, long jobId) {
    int updated =
        SessionUtils.doWithCommitAndFetchResult(
            TableMaintenanceActionMapper.class, mapper -> mapper.bindJobId(id, jobId));
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
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish timestamp millis
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "markTableMaintenanceActionFinished")
  public void markFinished(long jobId, Long afterSnapshotId, long finishedAt) {
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
   * @return all rows with {@code finished_at IS NULL} (may include terminal jobs not yet marked)
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
