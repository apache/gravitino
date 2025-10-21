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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.ibatis.annotations.Param;

public class TableColumnBaseSQLProvider {

  public String listColumnPOsByTableIdAndVersion(
      @Param("tableId") Long tableId, @Param("tableVersion") Long tableVersion) {
    return "SELECT t1.column_id AS columnId, t1.column_name AS columnName,"
        + " t1.column_position AS columnPosition,"
        + " t1.metalake_id AS metalakeId, t1.catalog_id AS catalogId,"
        + " t1.schema_id AS schemaId, t1.table_id AS tableId,"
        + " t1.table_version AS tableVersion, t1.column_type AS columnType,"
        + " t1.column_comment AS columnComment, t1.column_nullable AS nullable,"
        + " t1.column_auto_increment AS autoIncrement,"
        + " t1.column_default_value AS defaultValue, t1.column_op_type AS columnOpType,"
        + " t1.deleted_at AS deletedAt, t1.audit_info AS auditInfo"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " t1 JOIN ("
        + " SELECT column_id, MAX(table_version) AS max_table_version"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE table_id = #{tableId} AND table_version <= #{tableVersion} AND deleted_at = 0"
        + " GROUP BY column_id) t2"
        + " ON t1.column_id = t2.column_id AND t1.table_version = t2.max_table_version"
        + " AND t1.table_id = #{tableId}";
  }

  public String insertColumnPOs(@Param("columnPOs") List<ColumnPO> columnPOs) {
    return "<script>"
        + "INSERT INTO "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " (column_id, column_name, column_position, metalake_id, catalog_id, schema_id,"
        + " table_id, table_version,"
        + " column_type, column_comment, column_nullable, column_auto_increment,"
        + " column_default_value, column_op_type, deleted_at, audit_info)"
        + " VALUES "
        + "<foreach collection='columnPOs' item='item' separator=','>"
        + "(#{item.columnId}, #{item.columnName}, #{item.columnPosition}, #{item.metalakeId},"
        + " #{item.catalogId}, #{item.schemaId}, #{item.tableId}, #{item.tableVersion},"
        + " #{item.columnType}, #{item.columnComment}, #{item.nullable}, #{item.autoIncrement},"
        + " #{item.defaultValue}, #{item.columnOpType}, #{item.deletedAt}, #{item.auditInfo})"
        + "</foreach>"
        + "</script>";
  }

  public String updateSchemaIdByTableId(
      @Param("tableId") Long tableId, @Param("newSchemaId") Long newSchemaId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET schema_id = #{newSchemaId}"
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  public String softDeleteColumnsByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  public String softDeleteColumnsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteColumnsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteColumnsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String deleteColumnPOsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String selectColumnIdByTableIdAndName(
      @Param("tableId") Long tableId, @Param("columnName") String name) {
    return "SELECT"
        + "   CASE"
        + "     WHEN column_op_type = 3 THEN NULL"
        + "     ELSE column_id"
        + "   END"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE table_id = #{tableId} AND column_name = #{columnName} AND deleted_at = 0"
        + " ORDER BY table_version DESC LIMIT 1";
  }

  public String selectColumnPOById(@Param("columnId") Long columnId) {
    return "SELECT column_id AS columnId, column_name AS columnName,"
        + " column_position AS columnPosition, metalake_id AS metalakeId, catalog_id AS catalogId,"
        + " schema_id AS schemaId, table_id AS tableId,"
        + " table_version AS tableVersion, column_type AS columnType,"
        + " column_comment AS columnComment, column_nullable AS nullable,"
        + " column_auto_increment AS autoIncrement,"
        + " column_default_value AS defaultValue, column_op_type AS columnOpType,"
        + " deleted_at AS deletedAt, audit_info AS auditInfo"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE column_id = #{columnId} AND deleted_at = 0"
        + " ORDER BY table_version DESC LIMIT 1";
  }

  public String listColumnPOsByColumnIds(@Param("columnIds") List<Long> columnIds) {
    return "<script>"
        + " SELECT c.column_id AS columnId, c.column_name AS columnName,"
        + " c.column_position AS columnPosition, c.metalake_id AS metalakeId, c.catalog_id AS catalogId,"
        + " c.schema_id AS schemaId, c.table_id AS tableId,"
        + " c.table_version AS tableVersion, c.column_type AS columnType,"
        + " c.column_comment AS columnComment, c.column_nullable AS nullable,"
        + " c.column_auto_increment AS autoIncrement,"
        + " c.column_default_value AS defaultValue, c.column_op_type AS columnOpType,"
        + " c.deleted_at AS deletedAt, c.audit_info AS auditInfo"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " c"
        + " JOIN ("
        + "    SELECT column_id, MAX(table_version) AS max_version"
        + "    FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + "    WHERE column_id IN ("
        + "        <foreach collection='columnIds' item='columnId' separator=','>"
        + "          #{columnId}"
        + "        </foreach>"
        + "    ) AND deleted_at = 0 GROUP BY column_id"
        + " ) latest"
        + " ON c.column_id = latest.column_id AND c.table_version = latest.max_version"
        + " WHERE c.column_id IN ("
        + "<foreach collection='columnIds' item='columnId' separator=','>"
        + "#{columnId}"
        + "</foreach>"
        + ")"
        + " AND c.deleted_at = 0"
        + "</script>";
  }
}
