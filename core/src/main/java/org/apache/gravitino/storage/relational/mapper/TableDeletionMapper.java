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

import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/** Atomic, generation-scoped table tombstone and restore operations. */
public interface TableDeletionMapper {

  /** Locks the live parent namespace so same-name create, delete, and restore serialize. */
  @Select({
    "SELECT schema_id FROM schema_meta",
    "WHERE schema_id = #{schemaId} AND deleted_at = 0 FOR UPDATE"
  })
  Long lockLiveSchema(@Param("schemaId") long schemaId);

  /** Locks and returns the live table row under an already locked namespace. */
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE schema_id = #{schemaId} AND table_name = #{tableName}",
    "AND deleted_at = 0 FOR UPDATE"
  })
  TablePO selectLiveTableForUpdate(
      @Param("schemaId") long schemaId, @Param("tableName") String tableName);

  /** Returns the exact retained table row, including its deletion generation. */
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE table_id = #{tableId} AND deletion_id = #{deletionId} FOR UPDATE"
  })
  TablePO selectTableGenerationForUpdate(
      @Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Returns a live table only when it is the expected restored generation. */
  @Select({
    "SELECT table_id AS tableId, table_name AS tableName, metalake_id AS metalakeId,",
    "catalog_id AS catalogId, schema_id AS schemaId, audit_info AS auditInfo,",
    "current_version AS currentVersion, last_version AS lastVersion,",
    "deleted_at AS deletedAt, deletion_id AS deletionId FROM table_meta",
    "WHERE table_id = #{tableId} AND schema_id = #{schemaId}",
    "AND table_name = #{tableName} AND deleted_at = 0 AND deletion_id IS NULL"
  })
  TablePO selectRestoredTable(
      @Param("tableId") long tableId,
      @Param("schemaId") long schemaId,
      @Param("tableName") String tableName);

  /** Tombstones the exact live table row. */
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

  /** Stamps the live owner relation with the same deletion generation. */
  @Update({
    "UPDATE owner_meta SET deleted_at = #{deletedAt}, updated_at = #{deletedAt},",
    "deletion_id = #{deletionId}",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneOwnerRelations(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps all live column-version rows owned by the table. */
  @Update({
    "UPDATE table_column_version_info SET deleted_at = #{deletedAt},",
    "deletion_id = #{deletionId}",
    "WHERE table_id = #{tableId} AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneColumns(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps live role grants whose securable object is the table. */
  @Update({
    "UPDATE role_meta_securable_object SET deleted_at = #{deletedAt},",
    "deletion_id = #{deletionId}",
    "WHERE metadata_object_id = #{tableId} AND type = 'TABLE'",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneSecurableObjects(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps live tag relations for the table and its columns. */
  @Update({
    "UPDATE tag_relation_meta SET deleted_at = #{deletedAt}, deletion_id = #{deletionId}",
    "WHERE deleted_at = 0 AND deletion_id IS NULL AND (",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND EXISTS (SELECT 1",
    "FROM table_column_version_info tc WHERE tc.table_id = #{tableId}",
    "AND tc.column_id = tag_relation_meta.metadata_object_id)))"
  })
  int tombstoneTagRelations(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps live table statistics. */
  @Update({
    "UPDATE statistic_meta SET deleted_at = #{deletedAt}, deletion_id = #{deletionId}",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneStatistics(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps live policy relations for the table. */
  @Update({
    "UPDATE policy_relation_meta SET deleted_at = #{deletedAt}, deletion_id = #{deletionId}",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstonePolicyRelations(
      @Param("tableId") long tableId,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Stamps the current table-version row. */
  @Update({
    "UPDATE table_version_info SET deleted_at = #{deletedAt}, deletion_id = #{deletionId}",
    "WHERE table_id = #{tableId} AND version = #{tableVersion}",
    "AND deleted_at = 0 AND deletion_id IS NULL"
  })
  int tombstoneTableVersion(
      @Param("tableId") long tableId,
      @Param("tableVersion") long tableVersion,
      @Param("deletedAt") long deletedAt,
      @Param("deletionId") String deletionId);

  /** Restores owner relations stamped by the exact deletion generation. */
  @Update({
    "UPDATE owner_meta SET deleted_at = 0, updated_at = #{restoredAt}, deletion_id = NULL",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deletion_id = #{deletionId}"
  })
  int restoreOwnerRelations(
      @Param("tableId") long tableId,
      @Param("deletionId") String deletionId,
      @Param("restoredAt") long restoredAt);

  /** Restores column-version rows stamped by the exact deletion generation. */
  @Update({
    "UPDATE table_column_version_info SET deleted_at = 0, deletion_id = NULL",
    "WHERE table_id = #{tableId} AND deletion_id = #{deletionId}"
  })
  int restoreColumns(@Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Restores role grants stamped by the exact deletion generation. */
  @Update({
    "UPDATE role_meta_securable_object SET deleted_at = 0, deletion_id = NULL",
    "WHERE metadata_object_id = #{tableId} AND type = 'TABLE'",
    "AND deletion_id = #{deletionId}"
  })
  int restoreSecurableObjects(
      @Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Restores tag relations stamped by the exact deletion generation. */
  @Update({
    "UPDATE tag_relation_meta SET deleted_at = 0, deletion_id = NULL",
    "WHERE deletion_id = #{deletionId} AND (",
    "(metadata_object_type = 'TABLE' AND metadata_object_id = #{tableId}) OR",
    "(metadata_object_type = 'COLUMN' AND EXISTS (SELECT 1",
    "FROM table_column_version_info tc WHERE tc.table_id = #{tableId}",
    "AND tc.column_id = tag_relation_meta.metadata_object_id)))"
  })
  int restoreTagRelations(@Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Restores table statistics stamped by the exact deletion generation. */
  @Update({
    "UPDATE statistic_meta SET deleted_at = 0, deletion_id = NULL",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deletion_id = #{deletionId}"
  })
  int restoreStatistics(@Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Restores policy relations stamped by the exact deletion generation. */
  @Update({
    "UPDATE policy_relation_meta SET deleted_at = 0, deletion_id = NULL",
    "WHERE metadata_object_id = #{tableId} AND metadata_object_type = 'TABLE'",
    "AND deletion_id = #{deletionId}"
  })
  int restorePolicyRelations(
      @Param("tableId") long tableId, @Param("deletionId") String deletionId);

  /** Restores the current table-version row stamped by the exact deletion generation. */
  @Update({
    "UPDATE table_version_info SET deleted_at = 0, deletion_id = NULL",
    "WHERE table_id = #{tableId} AND version = #{tableVersion}",
    "AND deletion_id = #{deletionId}"
  })
  int restoreTableVersion(
      @Param("tableId") long tableId,
      @Param("tableVersion") long tableVersion,
      @Param("deletionId") String deletionId);

  /** Restores only the exact table ID and deletion generation. */
  @Update({
    "UPDATE table_meta SET deleted_at = 0, deletion_id = NULL",
    "WHERE table_id = #{tableId} AND schema_id = #{schemaId}",
    "AND table_name = #{tableName} AND current_version = #{tableVersion}",
    "AND deletion_id = #{deletionId}"
  })
  int restoreTable(
      @Param("tableId") long tableId,
      @Param("schemaId") long schemaId,
      @Param("tableName") String tableName,
      @Param("tableVersion") long tableVersion,
      @Param("deletionId") String deletionId);
}
