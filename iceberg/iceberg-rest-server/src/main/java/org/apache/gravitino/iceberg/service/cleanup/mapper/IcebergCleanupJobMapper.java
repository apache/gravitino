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

package org.apache.gravitino.iceberg.service.cleanup.mapper;

import java.util.List;
import org.apache.gravitino.iceberg.service.cleanup.po.IcebergCleanupJobPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * MyBatis mapper for the {@code iceberg_cleanup_job} table. SQL is supplied per backend by {@link
 * IcebergCleanupJobSQLProviderFactory} and executed through the Gravitino entity store's shared
 * {@code SqlSessionFactory}, so async cleanup reuses the relational backend's connection pool and
 * multi-backend handling instead of opening its own JDBC connections.
 */
public interface IcebergCleanupJobMapper {

  String TABLE_NAME = "iceberg_cleanup_job";

  @InsertProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "insertCleanupJob")
  void insertCleanupJob(@Param("po") IcebergCleanupJobPO po);

  @SelectProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "selectCandidateJobs")
  List<IcebergCleanupJobPO> selectCandidateJobs(
      @Param("heartbeatExpiry") long heartbeatExpiry, @Param("window") int window);

  @UpdateProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "markRunning")
  int markRunning(
      @Param("id") long id, @Param("now") long now, @Param("heartbeatExpiry") long heartbeatExpiry);

  @UpdateProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "markFinished")
  int markFinished(
      @Param("id") long id,
      @Param("state") String state,
      @Param("reason") String reason,
      @Param("now") long now,
      @Param("heartbeat") long heartbeat);

  @UpdateProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "recordFailure")
  int recordFailure(
      @Param("id") long id,
      @Param("reason") String reason,
      @Param("maxAttempts") int maxAttempts,
      @Param("now") long now,
      @Param("heartbeat") long heartbeat);

  @UpdateProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "heartbeat")
  int heartbeat(
      @Param("id") long id, @Param("lastHeartbeat") long lastHeartbeat, @Param("now") long now);

  @SelectProvider(
      type = IcebergCleanupJobSQLProviderFactory.class,
      method = "selectUnfinishedJobId")
  Long selectUnfinishedJobId(
      @Param("catalogId") long catalogId,
      @Param("namespace") String namespace,
      @Param("table") String table);

  @DeleteProvider(
      type = IcebergCleanupJobSQLProviderFactory.class,
      method = "deleteFinishedJobsByLegacyTimeline")
  int deleteFinishedJobsByLegacyTimeline(@Param("legacyTimeline") long legacyTimeline);

  @SelectProvider(type = IcebergCleanupJobSQLProviderFactory.class, method = "selectState")
  String selectState(@Param("id") long id);
}
