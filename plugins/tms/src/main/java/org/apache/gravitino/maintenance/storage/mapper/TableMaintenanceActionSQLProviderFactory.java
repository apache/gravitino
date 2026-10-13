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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.maintenance.storage.mapper.provider.base.TableMaintenanceActionBaseSQLProvider;
import org.apache.gravitino.maintenance.storage.mapper.provider.h2.TableMaintenanceActionH2Provider;
import org.apache.gravitino.maintenance.storage.mapper.provider.postgresql.TableMaintenanceActionPostgreSQLProvider;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL provider factory for {@link TableMaintenanceActionMapper}. */
public class TableMaintenanceActionSQLProviderFactory {

  private static final TableMaintenanceActionBaseSQLProvider MYSQL_PROVIDER =
      new TableMaintenanceActionBaseSQLProvider();
  private static final TableMaintenanceActionBaseSQLProvider H2_PROVIDER =
      new TableMaintenanceActionH2Provider();
  private static final TableMaintenanceActionBaseSQLProvider POSTGRESQL_PROVIDER =
      new TableMaintenanceActionPostgreSQLProvider();
  private static final Map<JDBCBackendType, TableMaintenanceActionBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private TableMaintenanceActionSQLProviderFactory() {}

  /**
   * @param actionMeta row to insert
   * @return INSERT statement
   */
  public static String insertAction(@Param("actionMeta") TableMaintenanceActionPO actionMeta) {
    return getProvider().insertAction(actionMeta);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL in milliseconds
   * @return UPDATE statement
   */
  public static String reclaimExpiredUnbound(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs) {
    return getProvider().reclaimExpiredUnbound(tableId, policyId, claimTimeoutMs);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public static String selectUnboundByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return getProvider().selectUnboundByTableAndPolicy(tableId, policyId);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param claimTimeoutMs unbound claim TTL in milliseconds
   * @return SELECT statement
   */
  public static String selectLiveUnboundByTableAndPolicy(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("claimTimeoutMs") long claimTimeoutMs) {
    return getProvider().selectLiveUnboundByTableAndPolicy(tableId, policyId, claimTimeoutMs);
  }

  /**
   * @param id action id
   * @param jobId job run id
   * @param expectedCreatedAt claim lease start observed at acquire time
   * @return UPDATE statement
   */
  public static String bindJobId(
      @Param("id") long id,
      @Param("jobId") long jobId,
      @Param("expectedCreatedAt") long expectedCreatedAt) {
    return getProvider().bindJobId(id, jobId, expectedCreatedAt);
  }

  /**
   * @param id action id
   * @return SELECT statement
   */
  public static String selectById(@Param("id") long id) {
    return getProvider().selectById(id);
  }

  /**
   * @param jobId job run id
   * @return SELECT statement
   */
  public static String selectByJobId(@Param("jobId") long jobId) {
    return getProvider().selectByJobId(jobId);
  }

  /**
   * @param jobId job run id
   * @param beforeSnapshotId snapshot id
   * @return UPDATE statement
   */
  public static String updateBeforeSnapshot(
      @Param("jobId") long jobId, @Param("beforeSnapshotId") long beforeSnapshotId) {
    return getProvider().updateBeforeSnapshot(jobId, beforeSnapshotId);
  }

  /**
   * @param jobId job run id
   * @param afterSnapshotId after snapshot
   * @param finishedAt finish millis
   * @return UPDATE statement
   */
  public static String markFinished(
      @Param("jobId") long jobId,
      @Param("afterSnapshotId") Long afterSnapshotId,
      @Param("finishedAt") long finishedAt) {
    return getProvider().markFinished(jobId, afterSnapshotId, finishedAt);
  }

  /**
   * @param id action id
   * @return DELETE statement
   */
  public static String deleteById(@Param("id") long id) {
    return getProvider().deleteById(id);
  }

  /**
   * @param jobId job run id
   * @return DELETE statement
   */
  public static String deleteByJobId(@Param("jobId") long jobId) {
    return getProvider().deleteByJobId(jobId);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public static String selectInFlightByTableAndPolicy(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return getProvider().selectInFlightByTableAndPolicy(tableId, policyId);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return SELECT statement
   */
  public static String selectMaxFinishedAt(
      @Param("tableId") long tableId, @Param("policyId") long policyId) {
    return getProvider().selectMaxFinishedAt(tableId, policyId);
  }

  static TableMaintenanceActionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDER_MAP.get(JDBCBackendType.fromString(databaseId));
  }
}
