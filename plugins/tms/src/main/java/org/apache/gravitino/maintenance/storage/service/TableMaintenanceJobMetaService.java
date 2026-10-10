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
   * Inserts an in-flight maintenance job row before {@code runJob}.
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
   * @param tableId table id
   * @param policyId policy id
   * @return whether a row exists with {@code finished_at IS NULL}
   */
  public boolean hasInFlightJob(long tableId, long policyId) {
    int count =
        SessionUtils.getWithoutCommit(
            TableMaintenanceJobMapper.class,
            mapper -> mapper.countInFlightByTableAndPolicy(tableId, policyId));
    return count > 0;
  }

  /**
   * @param id maintenance job row id
   * @param beforeSnapshotId sampled snapshot id
   */
  public void recordBeforeSnapshot(long id, long beforeSnapshotId) {
    SessionUtils.doWithCommit(
        TableMaintenanceJobMapper.class,
        mapper -> mapper.updateBeforeSnapshot(id, beforeSnapshotId));
  }

  /**
   * @param id maintenance job row id
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish timestamp millis
   */
  public void markFinished(long id, Long jobId, Long afterSnapshotId, long finishedAt) {
    SessionUtils.doWithCommit(
        TableMaintenanceJobMapper.class,
        mapper -> mapper.markFinished(id, jobId, afterSnapshotId, finishedAt));
  }
}
