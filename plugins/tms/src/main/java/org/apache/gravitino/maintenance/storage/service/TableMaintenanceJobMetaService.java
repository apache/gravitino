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

import org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceJobMapper;
import org.apache.gravitino.maintenance.storage.mapper.TableSnapshotMetricsMapper;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** DAO service for {@code table_maintenance_job}. */
public class TableMaintenanceJobMetaService {

  /**
   * Inserts a maintenance job row with {@code job_id}, {@code table_id}, and {@code policy_id}
   * (§5.2).
   *
   * @param jobId {@code job_run_meta.job_run_id}
   * @param tableId table entity id
   * @param policyId policy entity id
   */
  public void insertJob(long jobId, long tableId, long policyId) {
    TableMaintenanceJobPO po =
        TableMaintenanceJobPO.builder()
            .withJobId(jobId)
            .withTableId(tableId)
            .withPolicyId(policyId)
            .build();
    SessionUtils.doWithCommit(TableMaintenanceJobMapper.class, mapper -> mapper.insertJob(po));
  }

  /**
   * Records the sampled Iceberg snapshot id ({@code S0}) for a running job.
   *
   * @param jobId Gravitino job run id
   * @param beforeSnapshotId sampled snapshot id
   */
  public void recordBeforeSnapshot(long jobId, long beforeSnapshotId) {
    SessionUtils.doWithCommit(
        TableMaintenanceJobMapper.class,
        mapper -> mapper.updateBeforeSnapshot(jobId, beforeSnapshotId));
  }

  /**
   * Marks the job finished with the terminal snapshot id.
   *
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish timestamp millis
   */
  public void markFinished(long jobId, Long afterSnapshotId, long finishedAt) {
    SessionUtils.doWithCommit(
        TableMaintenanceJobMapper.class,
        mapper -> mapper.markFinished(jobId, afterSnapshotId, finishedAt));
  }

  /**
   * Deletes {@code table_snapshot_metrics} rows linked to the job's before/after snapshot ids, then
   * deletes the {@code table_maintenance_job} row for {@code jobId}.
   *
   * @param jobId Gravitino job run id
   */
  public void deleteByJobId(long jobId) {
    SessionUtils.doMultipleWithCommit(
        () ->
            SessionUtils.doWithoutCommit(
                TableSnapshotMetricsMapper.class, mapper -> mapper.deleteByJobId(jobId)),
        () ->
            SessionUtils.doWithoutCommit(
                TableMaintenanceJobMapper.class, mapper -> mapper.deleteByJobId(jobId)));
  }
}
