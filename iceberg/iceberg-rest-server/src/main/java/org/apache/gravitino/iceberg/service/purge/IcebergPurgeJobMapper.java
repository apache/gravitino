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

package org.apache.gravitino.iceberg.service.purge;

import java.util.List;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * MyBatis mapper for the {@code iceberg_cleanup_job} table. SQL is supplied per backend by {@link
 * IcebergPurgeJobSQLProviderFactory} and executed through the Gravitino entity store's shared
 * {@code SqlSessionFactory}, so async purge reuses the relational backend's connection pool and
 * multi-backend handling instead of opening its own JDBC connections.
 */
public interface IcebergPurgeJobMapper {

  String TABLE_NAME = "iceberg_cleanup_job";

  @InsertProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "insertPurgeJob")
  void insertPurgeJob(@Param("po") IcebergPurgeJobPO po);

  @SelectProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "selectClaimableIds")
  List<Long> selectClaimableIds(
      @Param("staleBefore") long staleBefore, @Param("window") int window);

  @UpdateProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "markRunning")
  int markRunning(
      @Param("id") long id, @Param("now") long now, @Param("staleBefore") long staleBefore);

  @SelectProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "selectById")
  IcebergPurgeJobPO selectById(@Param("id") long id);

  @UpdateProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "markSucceeded")
  int markSucceeded(@Param("id") long id, @Param("now") long now);

  @UpdateProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "markFailed")
  int markFailed(@Param("id") long id, @Param("reason") String reason, @Param("now") long now);

  @UpdateProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "recordFailure")
  int recordFailure(
      @Param("id") long id,
      @Param("reason") String reason,
      @Param("maxAttempts") int maxAttempts,
      @Param("now") long now);

  @UpdateProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "heartbeat")
  int heartbeat(
      @Param("id") long id, @Param("lastWritten") long lastWritten, @Param("now") long now);

  @SelectProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "selectActiveJobId")
  Long selectActiveJobId(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table);

  @DeleteProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "pruneFinishedBefore")
  int pruneFinishedBefore(@Param("updatedBefore") long updatedBefore);

  @SelectProvider(type = IcebergPurgeJobSQLProviderFactory.class, method = "selectState")
  String selectState(@Param("id") long id);
}
