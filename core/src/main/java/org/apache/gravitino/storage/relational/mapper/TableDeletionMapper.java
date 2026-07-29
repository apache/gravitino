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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.TableDeletionEntryPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Row-locked, generation-scoped mutations of a retained table root. */
public interface TableDeletionMapper {

  /**
   * Returns and locks one exact live table root.
   *
   * @param tableId immutable table identifier
   * @return live table root, or {@code null} when absent
   */
  @Nullable
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE table_id = #{tableId} AND deleted_at = 0 AND deletion_id IS NULL FOR UPDATE"
  })
  TablePO selectLiveTableForUpdate(@Param("tableId") long tableId);

  /**
   * Returns the table root that points to an exact deletion action.
   *
   * @param deletionId opaque deletion identifier
   * @return retained table root, or {@code null} when the pointer is absent
   */
  @Nullable
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE deletion_id = #{deletionId} AND deleted_at > 0"
  })
  TablePO selectRetainedTable(@Param("deletionId") String deletionId);

  /** Returns a bounded, unlocked projection of expired retained table deletions. */
  @Results(
      value = {
        @Result(property = "table.tableId", column = "table_id"),
        @Result(property = "table.tableName", column = "table_name"),
        @Result(property = "table.metalakeId", column = "metalake_id"),
        @Result(property = "table.catalogId", column = "catalog_id"),
        @Result(property = "table.schemaId", column = "schema_id"),
        @Result(property = "table.auditInfo", column = "audit_info"),
        @Result(property = "table.currentVersion", column = "current_version"),
        @Result(property = "table.lastVersion", column = "last_version"),
        @Result(property = "table.deletedAt", column = "deleted_at"),
        @Result(property = "table.deletionId", column = "table_deletion_id"),
        @Result(property = "deletion.deletionId", column = "action_deletion_id"),
        @Result(property = "deletion.state", column = "action_state"),
        @Result(property = "deletion.retentionExpiresAt", column = "action_retention_expires_at"),
        @Result(property = "deletion.purgeJobId", column = "action_purge_job_id")
      })
  @Select({
    "SELECT t.table_id, t.table_name, t.metalake_id, t.catalog_id, t.schema_id,",
    "t.audit_info, t.current_version, t.last_version, t.deleted_at,",
    "t.deletion_id AS table_deletion_id, d.deletion_id AS action_deletion_id,",
    "d.state AS action_state, d.retention_expires_at AS action_retention_expires_at,",
    "d.purge_job_id AS action_purge_job_id FROM entity_deletion d",
    "JOIN table_meta t ON t.deletion_id = d.deletion_id",
    "WHERE d.state = 'DELETED' AND d.purge_job_id IS NULL",
    "AND d.retention_expires_at IS NOT NULL AND d.retention_expires_at <= #{now}",
    "AND t.deleted_at > 0 AND t.deletion_id IS NOT NULL",
    "ORDER BY d.retention_expires_at, d.deletion_id LIMIT #{limit}"
  })
  List<TableDeletionEntryPO> selectEligibleRetainedTableDeletions(
      @Param("now") long now, @Param("limit") int limit);
  /** Returns retained table roots under one exact schema identity. */
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE schema_id = #{schemaId} AND deleted_at > 0 AND deletion_id IS NOT NULL",
    "ORDER BY table_name, deletion_id"
  })
  List<TablePO> selectRetainedTables(@Param("schemaId") long schemaId);

  /** Returns retained table roots for one exact schema identity and table name. */
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE schema_id = #{schemaId} AND table_name = #{tableName}",
    "AND deleted_at > 0 AND deletion_id IS NOT NULL",
    "ORDER BY deletion_id"
  })
  List<TablePO> selectRetainedTablesByName(
      @Param("schemaId") long schemaId, @Param("tableName") String tableName);

  /**
   * Returns and locks the table root that points to an exact deletion action.
   *
   * @param deletionId opaque deletion identifier
   * @return retained table root, or {@code null} when the pointer is absent
   */
  @Nullable
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE deletion_id = #{deletionId} AND deleted_at > 0 FOR UPDATE"
  })
  TablePO selectRetainedTableForUpdate(@Param("deletionId") String deletionId);

  /**
   * Returns an exact live table root after restoration.
   *
   * @param tableId immutable table identifier
   * @return live table root, or {@code null} when absent
   */
  @Nullable
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE table_id = #{tableId} AND deleted_at = 0 AND deletion_id IS NULL"
  })
  TablePO selectLiveTable(@Param("tableId") long tableId);

  /** Returns the unchanged user owner of one retained table identity. */
  @Nullable
  @Select({
    "SELECT u.user_name FROM owner_meta o",
    "JOIN user_meta u ON u.user_id = o.owner_id AND u.deleted_at = 0",
    "WHERE o.metadata_object_id = #{tableId} AND o.metadata_object_type = 'TABLE'",
    "AND o.owner_type = 'USER' AND o.deleted_at = 0"
  })
  String selectRetainedUserOwnerName(@Param("tableId") long tableId);

  /** Returns the unchanged group owner of one retained table identity. */
  @Nullable
  @Select({
    "SELECT g.group_name FROM owner_meta o",
    "JOIN group_meta g ON g.group_id = o.owner_id AND g.deleted_at = 0",
    "WHERE o.metadata_object_id = #{tableId} AND o.metadata_object_type = 'TABLE'",
    "AND o.owner_type = 'GROUP' AND o.deleted_at = 0"
  })
  String selectRetainedGroupOwnerName(@Param("tableId") long tableId);

  /**
   * Tombstones the exact live table root.
   *
   * @param tableId immutable table identifier
   * @param schemaId immutable parent schema identifier
   * @param tableName table name at deletion time
   * @param tableVersion metadata version read from the locked live row
   * @param deletedAt authoritative deletion time
   * @param deletionId opaque deletion identifier
   * @return number of updated rows
   */
  @Update({
    "UPDATE table_meta SET deleted_at = #{deletedAt}, deletion_id = #{deletionId}",
    "WHERE table_id = #{tableId} AND schema_id = #{schemaId}",
    "AND table_name = #{tableName} AND current_version = #{tableVersion}",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneTable(
      @Param("tableId") long tableId,
      @Param("schemaId") long schemaId,
      @Param("tableName") String tableName,
      @Param("tableVersion") long tableVersion,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /**
   * Reactivates only the root that still points to the exact deletion action.
   *
   * @param tableId immutable table identifier
   * @param deletionId opaque deletion identifier
   * @return number of updated rows
   */
  @Update({
    "UPDATE table_meta SET deleted_at = 0, deletion_id = NULL",
    "WHERE table_id = #{tableId} AND deletion_id = #{deletionId} AND deleted_at > 0"
  })
  int restoreTable(@Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Hard-deletes owner relations attached to an exact table identity or any of its columns. */
  @Delete({
    "DELETE FROM owner_meta WHERE",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND metadata_object_id IN",
    "(SELECT DISTINCT column_id FROM table_column_version_info WHERE table_id = #{tableId}))"
  })
  int deleteOwnedOwnerRelations(@Param("tableId") long tableId);

  /** Hard-deletes tag relations attached to an exact table identity or any of its columns. */
  @Delete({
    "DELETE FROM tag_relation_meta WHERE",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND metadata_object_id IN",
    "(SELECT DISTINCT column_id FROM table_column_version_info WHERE table_id = #{tableId}))"
  })
  int deleteOwnedTagRelations(@Param("tableId") long tableId);

  /** Hard-deletes policy relations attached to an exact table identity or any of its columns. */
  @Delete({
    "DELETE FROM policy_relation_meta WHERE",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND metadata_object_id IN",
    "(SELECT DISTINCT column_id FROM table_column_version_info WHERE table_id = #{tableId}))"
  })
  int deleteOwnedPolicyRelations(@Param("tableId") long tableId);

  /** Hard-deletes statistics attached to an exact table identity or any of its columns. */
  @Delete({
    "DELETE FROM statistic_meta WHERE",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND metadata_object_id IN",
    "(SELECT DISTINCT column_id FROM table_column_version_info WHERE table_id = #{tableId}))"
  })
  int deleteOwnedStatistics(@Param("tableId") long tableId);

  /** Hard-deletes role grants attached to an exact table identity or any of its columns. */
  @Delete({
    "DELETE FROM role_meta_securable_object WHERE",
    "(type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(type = 'COLUMN' AND metadata_object_id IN",
    "(SELECT DISTINCT column_id FROM table_column_version_info WHERE table_id = #{tableId}))"
  })
  int deleteOwnedSecurableRelations(@Param("tableId") long tableId);

  /** Hard-deletes partition statistics owned by an exact table identity. */
  @Delete("DELETE FROM partition_statistic_meta WHERE table_id = #{tableId}")
  int deleteOwnedPartitionStatistics(@Param("tableId") long tableId);

  /** Hard-deletes all table versions owned by an exact table identity. */
  @Delete("DELETE FROM table_version_info WHERE table_id = #{tableId}")
  int deleteOwnedTableVersions(@Param("tableId") long tableId);

  /** Hard-deletes all column versions owned by an exact table identity. */
  @Delete("DELETE FROM table_column_version_info WHERE table_id = #{tableId}")
  int deleteOwnedColumnVersions(@Param("tableId") long tableId);

  /**
   * Hard-deletes only the retained table root for the expected deletion generation.
   *
   * @param tableId immutable table identifier
   * @param deletionId opaque deletion identifier
   * @return number of deleted rows
   */
  @Delete({
    "DELETE FROM table_meta WHERE table_id = #{tableId}",
    "AND deletion_id = #{deletionId} AND deleted_at > 0"
  })
  int deleteRetainedTable(@Param("tableId") long tableId, @Param("deletionId") String deletionId);
}
