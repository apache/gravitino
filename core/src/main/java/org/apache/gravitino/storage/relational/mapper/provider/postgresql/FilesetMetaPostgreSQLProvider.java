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

import static org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper.META_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.FilesetMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.ibatis.annotations.Param;

public class FilesetMetaPostgreSQLProvider extends FilesetMetaBaseSQLProvider {
  @Override
  public String softDeleteFilesetMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteFilesetMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteFilesetMetasBySchemaIds(List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String softDeleteFilesetMetasByFilesetId(Long filesetId, Long occVersion) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE fileset_id = #{filesetId}"
        + " AND occ_version = #{occVersion} AND deleted_at = 0";
  }

  @Override
  public String deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + META_TABLE_NAME
        + " WHERE fileset_id IN (SELECT fileset_id FROM "
        + META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String insertFilesetMetaOnDuplicateKeyUpdate(FilesetPO filesetPO) {
    return "INSERT INTO "
        + META_TABLE_NAME
        + " (fileset_id, fileset_name, metalake_id,"
        + " catalog_id, schema_id, type, audit_info,"
        + " current_version, last_version, occ_version, deleted_at)"
        + " VALUES ("
        + " #{filesetMeta.filesetId},"
        + " #{filesetMeta.filesetName},"
        + " #{filesetMeta.metalakeId},"
        + " #{filesetMeta.catalogId},"
        + " #{filesetMeta.schemaId},"
        + " #{filesetMeta.type},"
        + " #{filesetMeta.auditInfo},"
        + " #{filesetMeta.currentVersion},"
        + " #{filesetMeta.lastVersion},"
        + " #{filesetMeta.occVersion},"
        + " #{filesetMeta.deletedAt}"
        + " )"
        // Overwrite is selected by name, and a create request normally carries a newly generated
        // ID. Target the natural key so PostgreSQL preserves the ID of the row being replaced, the
        // same behavior that MySQL and H2 provide for their duplicate-key upsert.
        + " ON CONFLICT(schema_id, fileset_name, deleted_at) DO UPDATE SET"
        + " fileset_name = #{filesetMeta.filesetName},"
        + " metalake_id = #{filesetMeta.metalakeId},"
        + " catalog_id = #{filesetMeta.catalogId},"
        + " schema_id = #{filesetMeta.schemaId},"
        + " type = #{filesetMeta.type},"
        + " audit_info = #{filesetMeta.auditInfo},"
        // An overwrite is a write observed by OCC, so it advances the OCC token from the stored
        // value instead of resetting the row to the initial version the create request carries.
        // The history version is left alone: it is the join key into fileset_version_info, and
        // this statement writes no snapshot to move it to.
        //
        // PostgreSQL requires the stored row to be qualified on the update side of ON CONFLICT.
        + " occ_version = "
        + META_TABLE_NAME
        + ".occ_version + 1,"
        + " deleted_at = #{filesetMeta.deletedAt}";
  }
}
