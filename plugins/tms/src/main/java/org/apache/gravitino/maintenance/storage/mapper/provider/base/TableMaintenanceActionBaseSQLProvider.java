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

import static org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceActionMapper.TABLE_NAME;

import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for {@code table_maintenance_action}. */
public class TableMaintenanceActionBaseSQLProvider {

  /**
   * Inserts an in-flight occupancy row before {@code runJob}. {@code job_id} stays null until bound
   * after submission (§5.2).
   *
   * @param actionMeta row to insert
   * @return INSERT statement
   */
  public String insertAction(@Param("actionMeta") TableMaintenanceActionPO actionMeta) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, policy_id, created_at)"
        + " VALUES (#{actionMeta.tableId}, #{actionMeta.policyId}, #{actionMeta.createdAt})";
  }

  /**
   * Binds the Gravitino job run id after {@code runJob} succeeds.
   *
   * @param id action row id
   * @param jobId {@code job_run_meta.job_run_id}
   * @return UPDATE statement
   */
  public String bindJobId(@Param("id") long id, @Param("jobId") long jobId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET job_id = #{jobId}"
        + " WHERE id = #{id} AND job_id IS NULL AND finished_at IS NULL";
  }

  /**
   * @param id action row id
   * @return SELECT statement
   */
  public String selectById(@Param("id") long id) {
    return selectColumns() + " WHERE id = #{id}";
  }

  /**
   * @param jobId Gravitino job run id
   * @return SELECT statement
   */
  public String selectByJobId(@Param("jobId") long jobId) {
    return selectColumns() + " WHERE job_id = #{jobId}";
  }

  /**
   * Sets {@code before_snapshot_id} once when {@code S0} is sampled.
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
        + " WHERE job_id = #{jobId} AND before_snapshot_id IS NULL AND finished_at IS NULL";
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
   * @param id action row id
   * @return DELETE statement
   */
  public String deleteById(@Param("id") long id) {
    return "DELETE FROM " + TABLE_NAME + " WHERE id = #{id}";
  }

  /**
   * @param jobId Gravitino job run id
   * @return DELETE statement
   */
  public String deleteByJobId(@Param("jobId") long jobId) {
    return "DELETE FROM " + TABLE_NAME + " WHERE job_id = #{jobId}";
  }

  /**
   * Returns all rows with {@code finished_at IS NULL} for {@code (table_id, policy_id)}.
   *
   * <p>Multiple open rows can exist because {@code OccupancyGate} does not backfill {@code
   * finished_at} when the Gravitino job is already terminal.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public String selectInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return selectColumns()
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND finished_at IS NULL";
  }

  /**
   * Returns {@code MAX(finished_at)} for {@code (table_id, policy_id)}.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public String selectMaxFinishedAt(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return "SELECT MAX(finished_at) FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND finished_at IS NOT NULL";
  }

  private static String selectColumns() {
    return "SELECT id, job_id AS jobId, table_id AS tableId, policy_id AS policyId,"
        + " before_snapshot_id AS beforeSnapshotId, after_snapshot_id AS afterSnapshotId,"
        + " finished_at AS finishedAt, created_at AS createdAt FROM "
        + TABLE_NAME;
  }
}
