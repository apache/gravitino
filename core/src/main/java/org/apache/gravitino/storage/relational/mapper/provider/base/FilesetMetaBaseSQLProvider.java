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

import static org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper.META_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper.VERSION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.ibatis.annotations.Param;

public class FilesetMetaBaseSQLProvider {

  public String listFilesetPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
        + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location_name, vi.storage_location,"
        + " vi.deleted_at as version_deleted_at"
        + " FROM "
        + META_TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
        + " WHERE fm.schema_id = #{schemaId} AND fm.deleted_at = 0 AND vi.deleted_at = 0";
  }

  public String listFilesetPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return """
        SELECT
            mm.metalake_id,
            cm.catalog_id,
            sm.schema_id,
            vi.fileset_id,
            fm.fileset_name,
            fm.type,
            fm.audit_info,
            fm.current_version,
            fm.last_version,
            fm.deleted_at,
            vi.id,
            vi.metalake_id as version_metalake_id,
            vi.catalog_id as version_catalog_id,
            vi.schema_id as version_schema_id,
            vi.fileset_id as version_fileset_id,
            vi.version,
            vi.fileset_comment,
            vi.properties,
            vi.storage_location_name,
            vi.storage_location,
            vi.deleted_at as version_deleted_at
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s fm ON sm.schema_id = fm.schema_id
            AND fm.deleted_at = 0
        LEFT JOIN
            %s vi ON fm.fileset_id = vi.fileset_id
            AND fm.current_version = vi.version
            AND vi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            META_TABLE_NAME,
            VERSION_TABLE_NAME);
  }

  public String selectFilesetIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return "SELECT fileset_id as filesetId FROM "
        + META_TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND fileset_name = #{filesetName}"
        + " AND deleted_at = 0";
  }

  public String listFilesetPOsByFilesetIds(@Param("filesetIds") List<Long> filesetIds) {
    return "<script>"
        + "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
        + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location_name, vi.storage_location,"
        + " vi.deleted_at as version_deleted_at"
        + " FROM "
        + META_TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
        + " WHERE fm.fileset_id IN ("
        + "<foreach collection='filesetIds' item='filesetId' separator=','>"
        + "#{filesetId}"
        + "</foreach>"
        + ") "
        + " AND fm.deleted_at = 0 AND vi.deleted_at = 0"
        + "</script>";
  }

  public String selectFilesetMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
        + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location_name, vi.storage_location,"
        + " vi.deleted_at as version_deleted_at"
        + " FROM "
        + META_TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
        + " WHERE fm.schema_id = #{schemaId} AND fm.fileset_name = #{filesetName}"
        + " AND fm.deleted_at = 0 AND vi.deleted_at = 0";
  }

  public String selectFilesetByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("filesetName") String filesetName) {
    return """
        SELECT
            mm.metalake_id,
            cm.catalog_id,
            sm.schema_id,
            vi.fileset_id,
            fm.fileset_name,
            fm.type,
            fm.audit_info,
            fm.current_version,
            fm.last_version,
            fm.deleted_at,
            vi.id,
            vi.metalake_id as version_metalake_id,
            vi.catalog_id as version_catalog_id,
            vi.schema_id as version_schema_id,
            vi.fileset_id as version_fileset_id,
            vi.version,
            vi.fileset_comment,
            vi.properties,
            vi.storage_location_name,
            vi.storage_location,
            vi.deleted_at as version_deleted_at
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s fm ON sm.schema_id = fm.schema_id
            AND fm.fileset_name = #{filesetName}
            AND fm.deleted_at = 0
        LEFT JOIN
            %s vi ON fm.fileset_id = vi.fileset_id
            AND fm.current_version = vi.version
            AND vi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            META_TABLE_NAME,
            VERSION_TABLE_NAME);
  }

  public String selectFilesetMetaById(@Param("filesetId") Long filesetId) {
    return "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
        + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location_name, vi.storage_location,"
        + " vi.deleted_at as version_deleted_at"
        + " FROM "
        + META_TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
        + " WHERE fm.fileset_id = #{filesetId}"
        + " AND fm.deleted_at = 0 AND vi.deleted_at = 0";
  }

  /**
   * Returns the active fileset metadata row selected by its natural key.
   *
   * <p>An overwrite may match the natural key instead of the incoming ID. Reading the stored row
   * after the upsert tells dependent version rows which ID and database-generated version to use.
   *
   * @param schemaId the schema ID
   * @param filesetName the fileset name
   * @return the metadata-only select SQL
   */
  public String selectFilesetMetaBySchemaIdAndNameForUpdate(
      @Param("schemaId") Long schemaId, @Param("filesetName") String filesetName) {
    return "SELECT fileset_id as filesetId, fileset_name as filesetName,"
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
        + " type as type, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + META_TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND fileset_name = #{filesetName}"
        + " AND deleted_at = 0 FOR UPDATE";
  }

  public String insertFilesetMeta(@Param("filesetMeta") FilesetPO filesetPO) {
    return "INSERT INTO "
        + META_TABLE_NAME
        + " (fileset_id, fileset_name, metalake_id,"
        + " catalog_id, schema_id, type, audit_info,"
        + " current_version, last_version, deleted_at)"
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
        + " #{filesetMeta.deletedAt}"
        + " )";
  }

  public String insertFilesetMetaOnDuplicateKeyUpdate(@Param("filesetMeta") FilesetPO filesetPO) {
    return "INSERT INTO "
        + META_TABLE_NAME
        + " (fileset_id, fileset_name, metalake_id,"
        + " catalog_id, schema_id, type, audit_info,"
        + " current_version, last_version, deleted_at)"
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
        + " #{filesetMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " fileset_name = #{filesetMeta.filesetName},"
        + " metalake_id = #{filesetMeta.metalakeId},"
        + " catalog_id = #{filesetMeta.catalogId},"
        + " schema_id = #{filesetMeta.schemaId},"
        + " type = #{filesetMeta.type},"
        + " audit_info = #{filesetMeta.auditInfo},"
        // An overwrite is also a write observed by OCC. Advance from the stored value instead of
        // resetting the row to the initial version carried by the incoming create request.
        //
        // Keep current_version last: MySQL evaluates these assignments left to right against the
        // columns already assigned, while H2 and PostgreSQL evaluate every right-hand side against
        // the row as it was before the update. Both agree only while current_version is read
        // before it is assigned.
        + " last_version = current_version + 1,"
        + " current_version = current_version + 1,"
        + " deleted_at = #{filesetMeta.deletedAt}";
  }

  /**
   * Returns SQL that updates a fileset only while its OCC version is unchanged and its next
   * snapshot version is free.
   *
   * <p>The version is the concurrency token, so payload, name, and audit columns are deliberately
   * excluded from the predicate. This also detects change-then-change-back races that a full-row
   * comparison would miss. The snapshot check detects rows affected by the legacy overwrite bug
   * without requiring a separate {@code MAX(version)} query on every normal alter.
   *
   * @param newFilesetPO the new fileset values
   * @param oldFilesetPO the fileset values and version observed by the caller
   * @return the version-checked update SQL
   */
  public String updateFilesetMeta(
      @Param("newFilesetMeta") FilesetPO newFilesetPO,
      @Param("oldFilesetMeta") FilesetPO oldFilesetPO) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET fileset_name = #{newFilesetMeta.filesetName},"
        + " metalake_id = #{newFilesetMeta.metalakeId},"
        + " catalog_id = #{newFilesetMeta.catalogId},"
        + " schema_id = #{newFilesetMeta.schemaId},"
        + " type = #{newFilesetMeta.type},"
        + " audit_info = #{newFilesetMeta.auditInfo},"
        + " current_version = #{newFilesetMeta.currentVersion},"
        + " last_version = #{newFilesetMeta.lastVersion},"
        + " deleted_at = #{newFilesetMeta.deletedAt}"
        + " WHERE fileset_id = #{oldFilesetMeta.filesetId}"
        + " AND current_version = #{oldFilesetMeta.currentVersion}"
        + " AND deleted_at = 0"
        + " AND NOT EXISTS (SELECT 1 FROM "
        + VERSION_TABLE_NAME
        + " fv WHERE fv.fileset_id = #{oldFilesetMeta.filesetId}"
        + " AND fv.version >= #{newFilesetMeta.currentVersion}"
        + " AND fv.deleted_at = 0)";
  }

  public String softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteFilesetMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  /**
   * Returns SQL that deletes only the fileset version observed by the caller.
   *
   * @param filesetId the fileset ID
   * @param currentVersion the version observed by the caller
   * @return the version-checked delete SQL
   */
  public String softDeleteFilesetMetasByFilesetId(
      @Param("filesetId") Long filesetId, @Param("currentVersion") Long currentVersion) {
    return "UPDATE "
        + META_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE fileset_id = #{filesetId}"
        + " AND current_version = #{currentVersion} AND deleted_at = 0";
  }

  public String deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + META_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String batchSelectFilesetByIdentifier(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("filesetNames") List<String> filesetNames) {
    return "<script>"
        + "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
        + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location_name, vi.storage_location,"
        + " vi.deleted_at as version_deleted_at"
        + " FROM "
        + META_TABLE_NAME
        + " fm"
        + " INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
        + " JOIN "
        + SchemaMetaMapper.TABLE_NAME
        + " sm ON fm.schema_id = sm.schema_id"
        + " JOIN "
        + CatalogMetaMapper.TABLE_NAME
        + " cm ON sm.catalog_id = cm.catalog_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON cm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND cm.catalog_name = #{catalogName}"
        + " AND sm.schema_name = #{schemaName}"
        + " AND fm.fileset_name IN ("
        + "<foreach collection='filesetNames' item='filesetName' separator=','>"
        + "#{filesetName}"
        + "</foreach>"
        + " )"
        + " AND fm.deleted_at = 0 AND vi.deleted_at = 0 AND sm.deleted_at = 0"
        + " AND cm.deleted_at = 0 AND mm.deleted_at = 0"
        + "</script>";
  }
}
