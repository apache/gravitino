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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for {@code table_maintenance_action}. */
public interface TableMaintenanceActionMapper {

  String TABLE_NAME = "table_maintenance_action";

  @InsertProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "insertAction")
  @Options(useGeneratedKeys = true, keyProperty = "actionMeta.id", keyColumn = "id")
  void insertAction(@Param("actionMeta") TableMaintenanceActionPO actionMeta);

  @UpdateProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "reclaimExpiredUnbound")
  int reclaimExpiredUnbound(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs);

  @Nullable
  @SelectProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "selectUnboundByTableAndPolicy")
  TableMaintenanceActionPO selectUnboundByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId);

  @Nullable
  @SelectProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "selectLiveUnboundByTableAndPolicy")
  TableMaintenanceActionPO selectLiveUnboundByTableAndPolicy(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs);

  @UpdateProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "bindJobId")
  int bindJobId(
      @Param("id") long id,
      @Param("jobId") long jobId,
      @Param("expectedCreatedAt") long expectedCreatedAt);

  @Nullable
  @SelectProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "selectById")
  TableMaintenanceActionPO selectById(@Param("id") long id);

  @Nullable
  @SelectProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "selectByJobId")
  TableMaintenanceActionPO selectByJobId(@Param("jobId") long jobId);

  @UpdateProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "updateBeforeSnapshot")
  int updateBeforeSnapshot(
      @Param("jobId") long jobId, @Param("beforeSnapshotId") long beforeSnapshotId);

  @UpdateProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "markFinished")
  int markFinished(
      @Param("jobId") long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt);

  @DeleteProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "deleteById")
  int deleteById(@Param("id") long id);

  @DeleteProvider(type = TableMaintenanceActionSQLProviderFactory.class, method = "deleteByJobId")
  int deleteByJobId(@Param("jobId") long jobId);

  @SelectProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "selectInFlightByTableAndPolicy")
  List<TableMaintenanceActionPO> selectInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId);

  @Nullable
  @SelectProvider(
      type = TableMaintenanceActionSQLProviderFactory.class,
      method = "selectMaxFinishedAt")
  Long selectMaxFinishedAt(@Param("tableId") long tableId, @Param("policyId") long policyId);
}
