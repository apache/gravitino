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
package org.apache.gravitino.maintenance.storage.mapper.provider.base;

import static org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceJobMapper.TABLE_NAME;

import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for {@code table_maintenance_job}. */
public class TableMaintenanceJobBaseSQLProvider {

  /**
   * @param po row to insert
   * @return INSERT statement
   */
  public String insertJob(@Param("po") TableMaintenanceJobPO po) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (job_id, table_id, policy_id, before_snapshot_id, after_snapshot_id, finished_at)"
        + " VALUES (#{po.jobId}, #{po.tableId}, #{po.policyId}, #{po.beforeSnapshotId},"
        + " #{po.afterSnapshotId}, #{po.finishedAt})";
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @return COUNT of in-flight rows
   */
  public String countInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return "SELECT COUNT(*) FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND finished_at IS NULL";
  }

  /**
   * @param id row id
   * @param beforeSnapshotId snapshot sampled in the job
   * @return UPDATE statement
   */
  public String updateBeforeSnapshot(
      @Param("id") long id, @Param("beforeSnapshotId") long beforeSnapshotId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET before_snapshot_id = #{beforeSnapshotId} WHERE id = #{id} AND finished_at IS NULL";
  }

  /**
   * @param id row id
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish time millis
   * @return UPDATE statement
   */
  public String markFinished(
      @Param("id") long id,
      @Param("jobId") Long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET job_id = COALESCE(job_id, #{jobId}), after_snapshot_id = #{afterSnapshotId},"
        + " finished_at = #{finishedAt} WHERE id = #{id} AND finished_at IS NULL";
  }
}
