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
import org.apache.gravitino.maintenance.storage.mapper.provider.base.TableMaintenanceJobBaseSQLProvider;
import org.apache.gravitino.maintenance.storage.mapper.provider.h2.TableMaintenanceJobH2Provider;
import org.apache.gravitino.maintenance.storage.mapper.provider.postgresql.TableMaintenanceJobPostgreSQLProvider;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceJobPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL provider factory for {@link TableMaintenanceJobMapper}. */
public class TableMaintenanceJobSQLProviderFactory {

  private static final TableMaintenanceJobBaseSQLProvider MYSQL_PROVIDER =
      new TableMaintenanceJobBaseSQLProvider();
  private static final TableMaintenanceJobBaseSQLProvider H2_PROVIDER =
      new TableMaintenanceJobH2Provider();
  private static final TableMaintenanceJobBaseSQLProvider POSTGRESQL_PROVIDER =
      new TableMaintenanceJobPostgreSQLProvider();
  private static final Map<JDBCBackendType, TableMaintenanceJobBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private TableMaintenanceJobSQLProviderFactory() {}

  /**
   * @param jobMeta row to insert
   * @return INSERT statement
   */
  public static String insertJob(@Param("jobMeta") TableMaintenanceJobPO jobMeta) {
    return getProvider().insertJob(jobMeta);
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
   * @param jobId job run id
   * @return DELETE statement
   */
  public static String deleteByJobId(@Param("jobId") long jobId) {
    return getProvider().deleteByJobId(jobId);
  }

  static TableMaintenanceJobBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDER_MAP.get(JDBCBackendType.fromString(databaseId));
  }
}
