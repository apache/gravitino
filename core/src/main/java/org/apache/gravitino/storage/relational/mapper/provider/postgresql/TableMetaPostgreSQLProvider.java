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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.TableMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.TableMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;

public class TableMetaPostgreSQLProvider extends TableMetaBaseSQLProvider {
  @Override
  public String insertTableMetaOnDuplicateKeyUpdate(TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + "(table_id, table_name, metalake_id,"
        + " catalog_id, schema_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
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
        + " ON CONFLICT (table_id) DO UPDATE SET "
        + " table_name = #{tableMeta.tableName},"
        + " metalake_id = #{tableMeta.metalakeId},"
        + " catalog_id = #{tableMeta.catalogId},"
        + " schema_id = #{tableMeta.schemaId},"
        + " audit_info = #{tableMeta.auditInfo},"
        + " current_version = #{tableMeta.currentVersion},"
        + " last_version = #{tableMeta.lastVersion},"
        + " deleted_at = #{tableMeta.deletedAt}";
  }

  @Override
  public String softDeleteTableMetasByTableId(Long tableId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTableMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTableMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteTableMetasBySchemaId(Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE table_id IN (SELECT table_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
