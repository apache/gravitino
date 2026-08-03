/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.mapper;

import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for active metadata deletion generations. */
public interface EntityDeletionMapper {

  /** Active deletion-generation table name. */
  String TABLE_NAME = "entity_deletion";

  /**
   * Inserts one active deletion generation.
   *
   * @param deletion deletion generation to insert
   */
  @InsertProvider(type = EntityDeletionSQLProvider.class, method = "insertEntityDeletion")
  void insertEntityDeletion(@Param("deletion") EntityDeletionPO deletion);

  /**
   * Selects one exact deletion generation.
   *
   * @param deletionId opaque deletion identifier
   * @return persisted deletion, or {@code null} when absent
   */
  @Nullable
  @SelectProvider(type = EntityDeletionSQLProvider.class, method = "selectEntityDeletion")
  EntityDeletionPO selectEntityDeletion(@Param("deletionId") String deletionId);

  /**
   * Selects and locks one exact deletion action.
   *
   * @param deletionId opaque deletion identifier
   * @return persisted deletion, or {@code null} when absent
   */
  @Nullable
  @SelectProvider(type = EntityDeletionSQLProvider.class, method = "selectEntityDeletionForUpdate")
  EntityDeletionPO selectEntityDeletionForUpdate(@Param("deletionId") String deletionId);

  /**
   * Claims an expired deletion generation for one exact purge job.
   *
   * @param deletionId opaque deletion identifier
   * @param purgeJobId durable cleanup-job identifier encoded as a decimal string
   * @param now authoritative server time used for the inclusive retention boundary
   * @return number of claimed rows
   */
  @UpdateProvider(type = EntityDeletionSQLProvider.class, method = "claimEntityDeletionForPurge")
  int claimEntityDeletionForPurge(
      @Param("deletionId") String deletionId,
      @Param("purgeJobId") String purgeJobId,
      @Param("now") long now);

  /**
   * Deletes an exact action after the caller has locked and validated it.
   *
   * @param deletionId opaque deletion identifier
   * @return number of deleted rows
   */
  @DeleteProvider(type = EntityDeletionSQLProvider.class, method = "deleteEntityDeletion")
  int deleteEntityDeletion(@Param("deletionId") String deletionId);

  /**
   * Deletes only the purge-owned action for an exact deletion generation and cleanup job.
   *
   * @param deletionId opaque deletion identifier
   * @param purgeJobId durable cleanup-job identifier encoded as a decimal string
   * @return number of deleted rows
   */
  @Delete({
    "DELETE FROM entity_deletion WHERE deletion_id = #{deletionId}",
    "AND state = 'PURGING' AND purge_job_id = #{purgeJobId}"
  })
  int deletePurgingEntityDeletion(
      @Param("deletionId") String deletionId, @Param("purgeJobId") String purgeJobId);
}
