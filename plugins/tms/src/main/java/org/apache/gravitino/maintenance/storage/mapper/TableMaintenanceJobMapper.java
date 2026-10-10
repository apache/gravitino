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
package org.apache.gravitino.maintenance.storage.mapper;

import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for {@code table_maintenance_job}. */
public interface TableMaintenanceJobMapper {

  String TABLE_NAME = "table_maintenance_job";

  @InsertProvider(type = TableMaintenanceJobSQLProviderFactory.class, method = "insertJob")
  @Options(useGeneratedKeys = true, keyProperty = "po.id", keyColumn = "id")
  void insertJob(@Param("po") TableMaintenanceJobPO po);

  @SelectProvider(
      type = TableMaintenanceJobSQLProviderFactory.class,
      method = "countInFlightByTableAndPolicy")
  int countInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId);

  @UpdateProvider(
      type = TableMaintenanceJobSQLProviderFactory.class,
      method = "updateBeforeSnapshot")
  int updateBeforeSnapshot(@Param("id") long id, @Param("beforeSnapshotId") long beforeSnapshotId);

  @UpdateProvider(type = TableMaintenanceJobSQLProviderFactory.class, method = "markFinished")
  int markFinished(
      @Param("id") long id,
      @Param("jobId") Long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt);
}
