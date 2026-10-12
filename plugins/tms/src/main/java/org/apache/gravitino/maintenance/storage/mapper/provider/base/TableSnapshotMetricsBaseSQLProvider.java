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

import org.apache.gravitino.maintenance.storage.mapper.TableMaintenanceJobMapper;
import org.apache.gravitino.maintenance.storage.mapper.TableSnapshotMetricsMapper;
import org.apache.gravitino.maintenance.storage.po.TableSnapshotMetricsPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for {@code table_snapshot_metrics}. */
public class TableSnapshotMetricsBaseSQLProvider {

  /**
   * @param po metrics row
   * @return INSERT statement
   */
  public String insertMetrics(@Param("po") TableSnapshotMetricsPO po) {
    return "INSERT INTO "
        + TableSnapshotMetricsMapper.TABLE_NAME
        + " (table_id, policy_id, snapshot_id, metrics_value, created_at)"
        + " VALUES (#{po.tableId}, #{po.policyId}, #{po.snapshotId}, #{po.metricsValue},"
        + " #{po.createdAt})";
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId Iceberg snapshot id
   * @return SELECT statement
   */
  public String selectMetrics(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("snapshotId") long snapshotId) {
    return "SELECT id, table_id AS tableId, policy_id AS policyId, snapshot_id AS snapshotId,"
        + " metrics_value AS metricsValue, created_at AS createdAt FROM "
        + TableSnapshotMetricsMapper.TABLE_NAME
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND snapshot_id ="
        + " #{snapshotId}";
  }

  /**
   * Deletes metrics rows tied to a maintenance job's before/after snapshot ids.
   *
   * @param jobId Gravitino job run id
   * @return DELETE statement
   */
  public String deleteByJobId(@Param("jobId") long jobId) {
    String metrics = TableSnapshotMetricsMapper.TABLE_NAME;
    String job = TableMaintenanceJobMapper.TABLE_NAME;
    return "DELETE FROM "
        + metrics
        + " WHERE EXISTS (SELECT 1 FROM "
        + job
        + " j WHERE j.job_id = #{jobId} AND j.table_id = "
        + metrics
        + ".table_id AND j.policy_id = "
        + metrics
        + ".policy_id AND (j.before_snapshot_id = "
        + metrics
        + ".snapshot_id OR j.after_snapshot_id = "
        + metrics
        + ".snapshot_id))";
  }
}
