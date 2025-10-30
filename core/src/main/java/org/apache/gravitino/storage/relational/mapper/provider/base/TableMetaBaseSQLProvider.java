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

import static org.apache.gravitino.storage.relational.mapper.TableMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.TableVersionMapper;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;

public class TableMetaBaseSQLProvider {

  public String listTablePOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version AND tv.deleted_at = 0"
        + " WHERE tm.schema_id = #{schemaId} AND tm.deleted_at = 0";
  }

  public String listTablePOsByTableIds(List<Long> tableIds) {
    return "<script>"
        + "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version AND tv.deleted_at = 0"
        + " WHERE tm.deleted_at = 0"
        + " AND tm.table_id IN ("
        + "<foreach collection='tableIds' item='tableId' separator=','>"
        + "#{tableId}"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  public String selectTableIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return "SELECT table_id as tableId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND table_name = #{tableName}"
        + " AND deleted_at = 0";
  }

  public String selectTableMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version AND tv.deleted_at = 0"
        + " WHERE tm.schema_id = #{schemaId} AND tm.table_name = #{tableName} AND tm.deleted_at = 0";
  }

  public String selectTableMetaById(@Param("tableId") Long tableId) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version"
        + " AND tv.deleted_at = 0"
        + " WHERE tm.table_id = #{tableId} AND tm.deleted_at = 0";
  }

  public String insertTableMeta(@Param("tableMeta") TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, table_name, metalake_id,"
        + " catalog_id, schema_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{tableMeta.tableId},"
        + " #{tableMeta.tableName},"
        + " #{tableMeta.metalakeId},"
        + " #{tableMeta.catalogId},"
        + " #{tableMeta.schemaId},"
        + " #{tableMeta.auditInfo},"
        + " #{tableMeta.currentVersion},"
        + " #{tableMeta.lastVersion},"
        + " #{tableMeta.deletedAt}"
        + " )";
  }

  public String insertTableMetaOnDuplicateKeyUpdate(@Param("tableMeta") TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, table_name, metalake_id,"
        + " catalog_id, schema_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{tableMeta.tableId},"
        + " #{tableMeta.tableName},"
        + " #{tableMeta.metalakeId},"
        + " #{tableMeta.catalogId},"
        + " #{tableMeta.schemaId},"
        + " #{tableMeta.auditInfo},"
        + " #{tableMeta.currentVersion},"
        + " #{tableMeta.lastVersion},"
        + " #{tableMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " table_name = #{tableMeta.tableName},"
        + " metalake_id = #{tableMeta.metalakeId},"
        + " catalog_id = #{tableMeta.catalogId},"
        + " schema_id = #{tableMeta.schemaId},"
        + " audit_info = #{tableMeta.auditInfo},"
        + " current_version = #{tableMeta.currentVersion},"
        + " last_version = #{tableMeta.lastVersion},"
        + " deleted_at = #{tableMeta.deletedAt}";
  }

  public String updateTableMeta(
      @Param("newTableMeta") TablePO newTablePO,
      @Param("oldTableMeta") TablePO oldTablePO,
      @Param("newSchemaId") Long newSchemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET table_name = #{newTableMeta.tableName},"
        + " metalake_id = #{newTableMeta.metalakeId},"
        + " catalog_id = #{newTableMeta.catalogId},"
        + " schema_id = #{newSchemaId},"
        + " audit_info = #{newTableMeta.auditInfo},"
        + " current_version = #{newTableMeta.currentVersion},"
        + " last_version = #{newTableMeta.lastVersion},"
        + " deleted_at = #{newTableMeta.deletedAt}"
        + " WHERE table_id = #{oldTableMeta.tableId}"
        + " AND table_name = #{oldTableMeta.tableName}"
        + " AND metalake_id = #{oldTableMeta.metalakeId}"
        + " AND catalog_id = #{oldTableMeta.catalogId}"
        + " AND schema_id = #{oldTableMeta.schemaId}"
        + " AND audit_info = #{oldTableMeta.auditInfo}"
        + " AND current_version = #{oldTableMeta.currentVersion}"
        + " AND last_version = #{oldTableMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeleteTableMetasByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
