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
   * Inserts a run row with {@code job_id}, {@code table_id}, and {@code policy_id} only. Snapshot
   * columns stay null until the Spark Job / terminal listener updates them (§5.2).
   *
   * @param jobMeta row to insert
   * @return INSERT statement
   */
  public String insertJob(@Param("jobMeta") TableMaintenanceJobPO jobMeta) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (job_id, table_id, policy_id)"
        + " VALUES (#{jobMeta.jobId}, #{jobMeta.tableId}, #{jobMeta.policyId})";
  }

  /**
   * Sets {@code before_snapshot_id} from inside the Spark Job when {@code S0} is sampled.
   *
   * @param jobId Gravitino job run id
   * @param beforeSnapshotId sampled snapshot id
   * @return UPDATE statement
   */
  public String updateBeforeSnapshot(
      @Param("jobId") long jobId, @Param("beforeSnapshotId") long beforeSnapshotId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET before_snapshot_id = #{beforeSnapshotId}"
        + " WHERE job_id = #{jobId} AND finished_at IS NULL";
  }

  /**
   * Sets terminal snapshot pointers when the Spark Job finishes.
   *
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish time millis
   * @return UPDATE statement
   */
  public String markFinished(
      @Param("jobId") long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET after_snapshot_id = #{afterSnapshotId}, finished_at = #{finishedAt}"
        + " WHERE job_id = #{jobId} AND finished_at IS NULL";
  }

  /**
   * @param jobId Gravitino job run id
   * @return DELETE statement
   */
  public String deleteByJobId(@Param("jobId") long jobId) {
    return "DELETE FROM " + TABLE_NAME + " WHERE job_id = #{jobId}";
  }
}
