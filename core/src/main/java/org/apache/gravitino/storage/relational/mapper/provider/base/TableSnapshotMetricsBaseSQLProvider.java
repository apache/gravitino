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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import static org.apache.gravitino.storage.relational.mapper.TableSnapshotMetricsMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.po.TableSnapshotMetricsPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for {@code table_snapshot_metrics}. */
public class TableSnapshotMetricsBaseSQLProvider {

  /**
   * @param po metrics row
   * @return INSERT statement
   */
  public String insertMetrics(@Param("po") TableSnapshotMetricsPO po) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, policy_id, snapshot_id, metrics_value)"
        + " VALUES (#{po.tableId}, #{po.policyId}, #{po.snapshotId}, #{po.metricsValue})";
  }

  /**
   * @param po metrics row with primary key fields set
   * @return UPDATE statement
   */
  public String updateMetrics(@Param("po") TableSnapshotMetricsPO po) {
    return "UPDATE "
        + TABLE_NAME
        + " SET metrics_value = #{po.metricsValue} WHERE table_id = #{po.tableId} AND policy_id ="
        + " #{po.policyId} AND snapshot_id = #{po.snapshotId}";
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
        + " metrics_value AS metricsValue FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND policy_id = #{policyId} AND snapshot_id ="
        + " #{snapshotId}";
  }
}
