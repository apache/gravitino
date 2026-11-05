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

import java.util.List;
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
   * Inserts unbound occupancy claims for a commit chain in one statement. Each {@code (table_id,
   * policy_id, finished_at=0)} must be free or the unique index rejects the whole batch.
   *
   * @param tableId table entity id (shared by all chain members)
   * @param policyIds policy entity ids in chain order
   * @return multi-row INSERT statement
   */
  public String insertActions(
      @Param("tableId") long tableId, @Param("policyIds") List<Long> policyIds) {
    String now = currentTimeMillisExpr();
    return "<script>"
        + "INSERT INTO "
        + TABLE_NAME
        + " (table_id, policy_id, created_at, finished_at) VALUES "
        + "<foreach item='policyId' collection='policyIds' separator=','>"
        + "(#{tableId}, #{policyId}, "
        + now
        + ", 0)"
        + "</foreach>"
        + "</script>";
  }

  /**
   * Abandons an expired unbound soft slot by setting {@code finished_at} to the database clock.
   * Callers then {@code INSERT} a new in-flight row (new {@code action_id}). Lease age uses the
   * database clock so multi-node TTL does not depend on JVM skew.
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
        + " SET finished_at = "
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
   * Unbound in-flight claims for a table (any policy).
   *
   * @param tableId table entity id
   * @return SELECT statement
   */
  public String selectUnboundByTableId(@Param("tableId") long tableId) {
    return selectColumns() + " WHERE table_id = #{tableId} AND job_id IS NULL AND finished_at = 0";
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
   * slot covers the whole run. {@code expectedCreatedAt} fences against a concurrent abandon that
   * closed this row and inserted a new {@code action_id}.
   *
   * @param actionId action row id
   * @param jobId {@code job_run_meta.job_run_id}
   * @param expectedCreatedAt claim lease start observed at acquire time
   * @return UPDATE statement
   */
  public String bindJobId(
      @Param("actionId") long actionId,
      @Param("jobId") long jobId,
      @Param("expectedCreatedAt") long expectedCreatedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET job_id = #{jobId}"
        + " WHERE action_id = #{actionId} AND job_id IS NULL AND finished_at = 0"
        + " AND created_at = #{expectedCreatedAt}";
  }

  /**
   * @param actionId action row id
   * @return SELECT statement
   */
  public String selectById(@Param("actionId") long actionId) {
    return selectColumns() + " WHERE action_id = #{actionId}";
  }

  /**
   * @param jobId Gravitino job run id
   * @return SELECT statement (one job may cover multiple tables)
   */
  public String selectByJobId(@Param("jobId") long jobId) {
    return selectColumns() + " WHERE job_id = #{jobId}";
  }

  /**
   * @param jobId Gravitino job run id
   * @param tableId table entity id
   * @return SELECT statement
   */
  public String selectByJobIdAndTableId(
      @Param("jobId") long jobId, @Param("tableId") long tableId) {
    return selectColumns() + " WHERE job_id = #{jobId} AND table_id = #{tableId}";
  }

  /**
   * @param jobId Gravitino job run id
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public String selectByJobIdAndTableIdAndPolicyId(
      @Param("jobId") long jobId,
      @Param("tableId") long tableId,
      @Param("policyId") long policyId) {
    return selectColumns()
        + " WHERE job_id = #{jobId} AND table_id = #{tableId} AND policy_id = #{policyId}";
  }

  /**
   * Sets {@code before_metrics_id} when {@code S0} is sampled ({@code IS NULL} first write).
   * Requires the soft slot to be bound ({@code job_id IS NOT NULL}) so unbound TTL cannot abandon
   * the row after a successful metrics write.
   *
   * @param actionId action row id
   * @param beforeMetricsId {@code table_snapshot_metrics.metrics_id} for the S0 sample
   * @return UPDATE statement
   */
  public String updateBeforeMetrics(
      @Param("actionId") long actionId, @Param("beforeMetricsId") long beforeMetricsId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET before_metrics_id = #{beforeMetricsId}"
        + " WHERE action_id = #{actionId}"
        + " AND job_id IS NOT NULL AND before_metrics_id IS NULL AND finished_at = 0";
  }

  /**
   * Sets {@code after_metrics_id} when {@code S1} is sampled ({@code IS NULL} first write) while
   * the soft slot is still open and bound.
   *
   * @param actionId action row id
   * @param afterMetricsId {@code table_snapshot_metrics.metrics_id} for the S1 sample
   * @return UPDATE statement
   */
  public String updateAfterMetrics(
      @Param("actionId") long actionId, @Param("afterMetricsId") long afterMetricsId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET after_metrics_id = #{afterMetricsId}"
        + " WHERE action_id = #{actionId}"
        + " AND job_id IS NOT NULL AND after_metrics_id IS NULL AND finished_at = 0";
  }

  /**
   * Releases the soft slot for one bound action row. Does not write metrics pointers; S0/S1 use
   * {@link #updateBeforeMetrics} / {@link #updateAfterMetrics}.
   *
   * <p>Only one in-flight row exists per {@code (table_id, policy_id)} ({@code finished_at = 0}),
   * so finish timestamps do not contend across concurrent submitters.
   *
   * @param actionId action row id
   * @param finishedAt finish time millis; must be {@code > 0}
   * @return UPDATE statement
   */
  public String markFinished(
      @Param("actionId") long actionId, @Param("finishedAt") long finishedAt) {
    return "UPDATE "
        + TABLE_NAME
        + " SET finished_at = #{finishedAt}"
        + " WHERE action_id = #{actionId} AND job_id IS NOT NULL AND finished_at = 0";
  }

  /**
   * @param actionId action row id
   * @return DELETE statement
   */
  public String deleteById(@Param("actionId") long actionId) {
    return "DELETE FROM " + TABLE_NAME + " WHERE action_id = #{actionId}";
  }

  /**
   * Deletes an unbound claim only when the lease {@code created_at} still matches. A concurrent
   * abandon that closed this row leaves the new owner's {@code action_id} intact.
   *
   * @param actionId action row id
   * @param expectedCreatedAt claim lease start observed at acquire time
   * @return DELETE statement
   */
  public String deleteUnboundClaim(
      @Param("actionId") long actionId, @Param("expectedCreatedAt") long expectedCreatedAt) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE action_id = #{actionId} AND job_id IS NULL AND finished_at = 0"
        + " AND created_at = #{expectedCreatedAt}";
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
    return "SELECT action_id AS actionId, job_id AS jobId, table_id AS tableId, policy_id AS policyId,"
        + " before_metrics_id AS beforeMetricsId, after_metrics_id AS afterMetricsId,"
        + " created_at AS createdAt, finished_at AS finishedAt FROM "
        + TABLE_NAME;
  }
}
