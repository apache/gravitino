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

import com.google.common.base.Preconditions;
import org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceJobMapper;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** DAO service for {@code table_maintenance_job}. */
public class TableMaintenanceJobMetaService {

  /**
   * Inserts a maintenance job row before {@code runJob} (§5.2). {@code job_id} stays null until
   * {@link #bindJobId(long, long)}.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return generated row id
   */
  public long insertInFlightJob(long tableId, long policyId) {
    TableMaintenanceJobPO po =
        TableMaintenanceJobPO.builder().withTableId(tableId).withPolicyId(policyId).build();
    SessionUtils.doWithCommit(TableMaintenanceJobMapper.class, mapper -> mapper.insertJob(po));
    Preconditions.checkState(po.id() != null, "insert did not return generated id");
    return po.id();
  }

  /**
   * Sets {@code job_id} after {@code runJob} returns a job run id.
   *
   * @param id {@code table_maintenance_job.id}
   * @param jobId {@code job_run_meta.job_run_id}
   */
  public void bindJobId(long id, long jobId) {
    SessionUtils.doWithCommit(
        TableMaintenanceJobMapper.class, mapper -> mapper.updateJobId(id, jobId));
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
}
