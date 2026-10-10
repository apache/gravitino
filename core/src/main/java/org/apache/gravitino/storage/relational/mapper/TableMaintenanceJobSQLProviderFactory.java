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
package org.apache.gravitino.storage.relational.mapper;

import org.apache.gravitino.storage.relational.mapper.provider.base.TableMaintenanceJobBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TableMaintenanceJobPO;
import org.apache.ibatis.annotations.Param;

/** SQL provider factory for {@link TableMaintenanceJobMapper}. */
public class TableMaintenanceJobSQLProviderFactory {

  private static final TableMaintenanceJobBaseSQLProvider PROVIDER =
      new TableMaintenanceJobBaseSQLProvider();

  private TableMaintenanceJobSQLProviderFactory() {}

  /**
   * @param po row to insert
   * @return INSERT statement
   */
  public static String insertJob(@Param("po") TableMaintenanceJobPO po) {
    return PROVIDER.insertJob(po);
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @return COUNT statement
   */
  public static String countInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return PROVIDER.countInFlightByTableAndPolicy(tableId, policyId);
  }

  /**
   * @param id row id
   * @param beforeSnapshotId snapshot id
   * @return UPDATE statement
   */
  public static String updateBeforeSnapshot(
      @Param("id") long id, @Param("beforeSnapshotId") long beforeSnapshotId) {
    return PROVIDER.updateBeforeSnapshot(id, beforeSnapshotId);
  }

  /**
   * @param id row id
   * @param jobId job run id
   * @param afterSnapshotId after snapshot
   * @param finishedAt finish millis
   * @return UPDATE statement
   */
  public static String markFinished(
      @Param("id") long id,
      @Param("jobId") Long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt) {
    return PROVIDER.markFinished(id, jobId, afterSnapshotId, finishedAt);
  }
}
