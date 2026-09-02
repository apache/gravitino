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

import static org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper.TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper.VERSION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.ibatis.annotations.Param;

public class FunctionMetaBaseSQLProvider {

  public String listFunctionPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return """
        SELECT
            mm.metalake_id,
            cm.catalog_id,
            sm.schema_id,
            fm.function_id,
            fm.function_name,
            fm.function_type,
            fm.deterministic,
            fm.function_current_version,
            fm.function_latest_version,
            fm.audit_info,
            fm.deleted_at,
            vi.id,
            vi.metalake_id as version_metalake_id,
            vi.catalog_id as version_catalog_id,
            vi.schema_id as version_schema_id,
            vi.function_id as version_function_id,
            vi.version,
            vi.function_comment,
            vi.definitions,
            vi.audit_info as version_audit_info,
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
            %s vi ON fm.function_id = vi.function_id
            AND fm.function_current_version = vi.version
            AND vi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0
        """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            VERSION_TABLE_NAME);
  }

  public String insertFunctionMeta(@Param("functionMeta") FunctionPO functionPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (function_id, function_name, metalake_id, catalog_id, schema_id,"
        + " function_type, `deterministic`, function_current_version,"
        + " function_latest_version, audit_info, deleted_at)"
        + " VALUES (#{functionMeta.functionId}, #{functionMeta.functionName},"
        + " #{functionMeta.metalakeId}, #{functionMeta.catalogId}, #{functionMeta.schemaId},"
        + " #{functionMeta.functionType}, #{functionMeta.deterministic},"
        + " #{functionMeta.functionCurrentVersion},"
        + " #{functionMeta.functionLatestVersion}, #{functionMeta.auditInfo},"
        + " #{functionMeta.deletedAt})";
  }

  public String insertFunctionMetaOnDuplicateKeyUpdate(
      @Param("functionMeta") FunctionPO functionPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (function_id, function_name, metalake_id, catalog_id, schema_id,"
        + " function_type, `deterministic`,"
        + " function_current_version, function_latest_version, audit_info, deleted_at)"
        + " VALUES (#{functionMeta.functionId}, #{functionMeta.functionName},"
        + " #{functionMeta.metalakeId}, #{functionMeta.catalogId}, #{functionMeta.schemaId},"
        + " #{functionMeta.functionType}, #{functionMeta.deterministic},"
        + " #{functionMeta.functionCurrentVersion},"
        + " #{functionMeta.functionLatestVersion}, #{functionMeta.auditInfo},"
        + " #{functionMeta.deletedAt})"
        + " ON DUPLICATE KEY UPDATE"
        + " function_name = #{functionMeta.functionName},"
        + " metalake_id = #{functionMeta.metalakeId},"
        + " catalog_id = #{functionMeta.catalogId},"
        + " schema_id = #{functionMeta.schemaId},"
        + " function_type = #{functionMeta.functionType},"
        + " `deterministic` = #{functionMeta.deterministic},"
        // Keep both version columns monotonic on overwrite. Assign latest first so MySQL computes
        // both values from the stored current version rather than the newly assigned value.
        + " function_latest_version = function_current_version + 1,"
        + " function_current_version = function_current_version + 1,"
        + " audit_info = #{functionMeta.auditInfo},"
        + " deleted_at = #{functionMeta.deletedAt}";
  }

  public String selectFunctionMetaByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("functionName") String functionName) {
    return """
        SELECT
            mm.metalake_id,
            cm.catalog_id,
            sm.schema_id,
            fm.function_id,
            fm.function_name,
            fm.function_type,
            fm.deterministic,
            fm.function_current_version,
            fm.function_latest_version,
            fm.audit_info,
            fm.deleted_at,
            vi.id,
            vi.metalake_id as version_metalake_id,
            vi.catalog_id as version_catalog_id,
            vi.schema_id as version_schema_id,
            vi.function_id as version_function_id,
            vi.version,
            vi.function_comment,
            vi.definitions,
            vi.audit_info as version_audit_info,
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
            AND fm.function_name = #{functionName}
            AND fm.deleted_at = 0
        INNER JOIN
            %s vi ON fm.function_id = vi.function_id
            AND fm.function_current_version = vi.version
            AND vi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0
        """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            VERSION_TABLE_NAME);
  }

  public String listFunctionPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT fm.function_id, fm.function_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.function_type, fm.`deterministic`,"
        + " fm.function_current_version, fm.function_latest_version,"
        + " fm.audit_info, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.function_id as version_function_id,"
        + " vi.version, vi.function_comment, vi.definitions,"
        + " vi.audit_info as version_audit_info, vi.deleted_at as version_deleted_at"
        + " FROM "
        + TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.function_id = vi.function_id AND fm.function_current_version = vi.version"
        + " WHERE fm.schema_id = #{schemaId} AND fm.deleted_at = 0 AND vi.deleted_at = 0";
  }

  /**
   * Returns the active function metadata row and holds it exclusively for the transaction.
   *
   * <p>The version table is deliberately not joined: PostgreSQL rejects locking the nullable side
   * of an outer join, and conflict classification only needs the root row's identity and version.
   *
   * @param functionId the function ID
   * @return the locking select SQL
   */
  public String selectFunctionMetaByIdForUpdate(@Param("functionId") Long functionId) {
    return "SELECT function_id as functionId, function_name as functionName,"
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
        + " function_current_version as functionCurrentVersion,"
        + " function_latest_version as functionLatestVersion,"
        + " audit_info as auditInfo, deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE function_id = #{functionId} AND deleted_at = 0 FOR UPDATE";
  }

  public String listFunctionPOsByFunctionIds(@Param("functionIds") List<Long> functionIds) {
    return "<script>"
        + " SELECT function_id, function_name, schema_id"
        + " FROM "
        + TABLE_NAME
        + " WHERE deleted_at = 0"
        + " AND function_id IN ("
        + "<foreach collection='functionIds' item='functionId' separator=','>"
        + "#{functionId}"
        + "</foreach>"
        + ") "
        + "</script>";
  }

  public String selectFunctionMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("functionName") String functionName) {
    return "SELECT fm.function_id, fm.function_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
        + " fm.function_type, fm.`deterministic`,"
        + " fm.function_current_version, fm.function_latest_version,"
        + " fm.audit_info, fm.deleted_at,"
        + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
        + " vi.schema_id as version_schema_id, vi.function_id as version_function_id,"
        + " vi.version, vi.function_comment, vi.definitions,"
        + " vi.audit_info as version_audit_info, vi.deleted_at as version_deleted_at"
        + " FROM "
        + TABLE_NAME
        + " fm INNER JOIN "
        + VERSION_TABLE_NAME
        + " vi ON fm.function_id = vi.function_id AND fm.function_current_version = vi.version"
        + " WHERE fm.schema_id = #{schemaId} AND fm.function_name = #{functionName}"
        + " AND fm.deleted_at = 0 AND vi.deleted_at = 0";
  }

  /**
   * Returns SQL that locks an active function by natural key without joining its version row.
   *
   * <p>This query is reserved for overwrite decisions. Normal reads keep using the inner-joined
   * query above so a broken current-version invariant is reported as missing instead of producing a
   * partially populated {@code FunctionPO}.
   */
  public String selectFunctionMetaBySchemaIdAndNameForUpdate(
      @Param("schemaId") Long schemaId, @Param("functionName") String functionName) {
    return "SELECT function_id as functionId, function_name as functionName,"
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
        + " function_current_version as functionCurrentVersion,"
        + " function_latest_version as functionLatestVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND function_name = #{functionName}"
        + " AND deleted_at = 0 FOR UPDATE";
  }

  public String selectFunctionIdBySchemaIdAndFunctionName(
      @Param("schemaId") Long schemaId, @Param("functionName") String functionName) {
    return "SELECT function_id"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND function_name = #{functionName} AND deleted_at = 0";
  }

  /**
   * Returns SQL that deletes only the function version observed by the caller.
   *
   * @param functionId the function ID
   * @param currentVersion the version observed by the caller
   * @return the version-checked delete SQL
   */
  public String softDeleteFunctionMetaByFunctionId(
      @Param("functionId") Long functionId, @Param("currentVersion") Integer currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE function_id = #{functionId}"
        + " AND function_current_version = #{currentVersion} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  public String deleteFunctionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /**
   * Returns SQL that updates a function only while its OCC version is unchanged.
   *
   * @param newFunctionPO the function values to write
   * @param oldFunctionPO the function values and OCC version read by the caller
   * @return the version-checked update SQL
   */
  public String updateFunctionMeta(
      @Param("newFunctionMeta") FunctionPO newFunctionPO,
      @Param("oldFunctionMeta") FunctionPO oldFunctionPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET function_name = #{newFunctionMeta.functionName},"
        + " metalake_id = #{newFunctionMeta.metalakeId},"
        + " catalog_id = #{newFunctionMeta.catalogId},"
        + " schema_id = #{newFunctionMeta.schemaId},"
        + " function_type = #{newFunctionMeta.functionType},"
        + " `deterministic` = #{newFunctionMeta.deterministic},"
        + " function_current_version = #{newFunctionMeta.functionCurrentVersion},"
        + " function_latest_version = #{newFunctionMeta.functionLatestVersion},"
        + " audit_info = #{newFunctionMeta.auditInfo},"
        + " deleted_at = #{newFunctionMeta.deletedAt}"
        + " WHERE function_id = #{oldFunctionMeta.functionId}"
        + " AND function_current_version = #{oldFunctionMeta.functionCurrentVersion}"
        + " AND deleted_at = 0";
  }
}
