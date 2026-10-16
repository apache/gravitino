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
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for {@code table_maintenance_action} (MySQL / H2 {@code MODE=MYSQL}). */
public class TableMaintenanceActionBaseSQLProvider {

  /**
   * @return SQL expression for the database clock in epoch millis
   */
  protected String currentTimeMillisExpr() {
    return DatabaseTimeSQL.MYSQL;
  }

  /**
   * Inserts an unbound occupancy claim before {@code runJob}. {@code finished_at = 0} holds the
   * exclusive soft slot (same pattern as {@code deleted_at}); {@code created_at} is the claim lease
   * start from the database clock.
   *
   * @param actionMeta row to insert ({@code createdAt} is ignored; DB time is used)
   * @return INSERT statement
   */
  public String insertAction(@Param("actionMeta") TableMaintenanceActionPO actionMeta) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, policy_id, created_at, finished_at)"
        + " VALUES (#{actionMeta.tableId}, #{actionMeta.policyId}, "
        + currentTimeMillisExpr()
        + ", 0)";
  }

  /**
   * Extends an expired unbound claim lease by CAS on {@code created_at} (ShedLock-style). Lease age
   * is evaluated against the database clock so every node shares one TTL timeline.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL in milliseconds
   * @return UPDATE statement
   */
  public String reclaimExpiredUnbound(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs) {
    String now = currentTimeMillisExpr();
    return "UPDATE "
        + TABLE_NAME
        + " SET created_at = "
        + now
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId}"
        + " AND job_id IS NULL AND finished_at = 0"
        + " AND created_at <= ("
        + now
        + ") - #{claimTimeoutMs}";
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement for the unbound claim row
   */
  public String selectUnboundByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return selectColumns()
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId}"
        + " AND job_id IS NULL AND finished_at = 0";
  }

  /**
   * Live unbound claim still within TTL on the database clock.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL in milliseconds
   * @return SELECT statement
   */
  public String selectLiveUnboundByTableAndPolicy(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs) {
    return selectColumns()
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId}"
        + " AND job_id IS NULL AND finished_at = 0"
        + " AND created_at > ("
        + currentTimeMillisExpr()
        + ") - #{claimTimeoutMs}";
  }

  /**
   * Binds the Gravitino job run id after {@code runJob}. Keeps {@code finished_at = 0} so the soft
   * slot covers the whole run. {@code expectedCreatedAt} fences against a concurrent reclaim that
   * CAS-updated {@code created_at}.
   *
   * @param id action row id
   * @param jobId {@code job_run_meta.job_run_id}
   * @param expectedCreatedAt claim lease start observed at acquire time
   * @return UPDATE statement
   */
  public String bindJobId(
      @Param("id") long id,
      @Param("jobId") long jobId,
      @Param("expectedCreatedAt") long expectedCreatedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET job_id = #{jobId}"
        + " WHERE id = #{id} AND job_id IS NULL AND finished_at = 0"
        + " AND created_at = #{expectedCreatedAt}";
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
   * Sets {@code before_snapshot_id} when {@code S0} is sampled ({@code IS NULL} first write).
   * Callers treat an already-matching value as an idempotent no-op in Java.
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
        + " WHERE job_id = #{jobId} AND before_snapshot_id IS NULL AND finished_at = 0";
  }

  /**
   * Sets terminal snapshot pointers when the Spark Job finishes and releases the soft slot.
   *
   * <p>Only one in-flight row exists per {@code (table_id, policy_id)} ({@code finished_at = 0}),
   * so finish timestamps do not contend across concurrent submitters.
   *
   * @param jobId Gravitino job run id
   * @param afterSnapshotId terminal snapshot id
   * @param finishedAt finish time millis; must be {@code > 0}
   * @return UPDATE statement
   */
  public String markFinished(
      @Param("jobId") long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET after_snapshot_id = #{afterSnapshotId}, finished_at = #{finishedAt}"
        + " WHERE job_id = #{jobId} AND finished_at = 0";
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
   * Returns the open row ({@code finished_at = 0}) for {@code (table_id, policy_id)}, if any.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public String selectInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return selectColumns()
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND finished_at = 0";
  }

  /**
   * Returns {@code MAX(finished_at)} among completed rows for {@code (table_id, policy_id)}.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public String selectMaxFinishedAt(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return "SELECT MAX(finished_at) FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND finished_at > 0";
  }

  private static String selectColumns() {
    return "SELECT id, job_id AS jobId, table_id AS tableId, policy_id AS policyId,"
        + " before_snapshot_id AS beforeSnapshotId, after_snapshot_id AS afterSnapshotId,"
        + " finished_at AS finishedAt, created_at AS createdAt FROM "
        + TABLE_NAME;
  }
}
