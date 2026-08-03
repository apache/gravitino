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

import static org.apache.gravitino.storage.relational.mapper.EntityDeletionMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL provider for active metadata deletion generations. */
public class EntityDeletionSQLProvider {

  private static final String SELECT_COLUMNS =
      "deletion_id AS deletionId, state,"
          + " retention_expires_at AS retentionExpiresAt, purge_job_id AS purgeJobId";

  /**
   * Builds the insert for one deletion generation.
   *
   * @param deletion deletion generation to insert
   * @return parameterized insert SQL
   */
  public static String insertEntityDeletion(@Param("deletion") EntityDeletionPO deletion) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (deletion_id, state, retention_expires_at, purge_job_id)"
        + " VALUES (#{deletion.deletionId}, #{deletion.state},"
        + " #{deletion.retentionExpiresAt}, #{deletion.purgeJobId})";
  }

  /**
   * Builds an exact deletion-generation lookup.
   *
   * @param deletionId opaque deletion identifier
   * @return parameterized select SQL
   */
  public static String selectEntityDeletion(@Param("deletionId") String deletionId) {
    return "SELECT "
        + SELECT_COLUMNS
        + " FROM "
        + TABLE_NAME
        + " WHERE deletion_id = #{deletionId}";
  }

  /**
   * Builds an exact deletion-action locking lookup.
   *
   * @param deletionId opaque deletion identifier
   * @return parameterized select SQL
   */
  public static String selectEntityDeletionForUpdate(@Param("deletionId") String deletionId) {
    return selectEntityDeletion(deletionId) + " FOR UPDATE";
  }

  /**
   * Builds the guarded transition that hands an expired deletion to one purge job.
   *
   * @param deletionId opaque deletion identifier
   * @param purgeJobId durable cleanup-job identifier encoded as a decimal string
   * @param now authoritative server time used for the inclusive retention boundary
   * @return parameterized update SQL
   */
  public static String claimEntityDeletionForPurge(
      @Param("deletionId") String deletionId,
      @Param("purgeJobId") String purgeJobId,
      @Param("now") long now) {
    return "UPDATE "
        + TABLE_NAME
        + " SET state = 'PURGING', purge_job_id = #{purgeJobId}"
        + " WHERE deletion_id = #{deletionId} AND state = 'DELETED'"
        + " AND purge_job_id IS NULL AND retention_expires_at IS NOT NULL"
        + " AND retention_expires_at <= #{now}";
  }

  /**
   * Builds an exact deletion-action removal after it has been locked and validated.
   *
   * @param deletionId opaque deletion identifier
   * @return parameterized delete SQL
   */
  public static String deleteEntityDeletion(@Param("deletionId") String deletionId) {
    return "DELETE FROM " + TABLE_NAME + " WHERE deletion_id = #{deletionId}";
  }
}
