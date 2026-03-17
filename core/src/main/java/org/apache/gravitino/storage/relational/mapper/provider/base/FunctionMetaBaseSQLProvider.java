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

import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
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
        + " function_current_version = #{functionMeta.functionCurrentVersion},"
        + " function_latest_version = #{functionMeta.functionLatestVersion},"
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

  public String softDeleteFunctionMetaByFunctionId(@Param("functionId") Long functionId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE function_id = #{functionId} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteFunctionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String deleteFunctionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

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
        + " AND function_name = #{oldFunctionMeta.functionName}"
        + " AND metalake_id = #{oldFunctionMeta.metalakeId}"
        + " AND catalog_id = #{oldFunctionMeta.catalogId}"
        + " AND schema_id = #{oldFunctionMeta.schemaId}"
        + " AND function_type = #{oldFunctionMeta.functionType}"
        + " AND function_current_version = #{oldFunctionMeta.functionCurrentVersion}"
        + " AND function_latest_version = #{oldFunctionMeta.functionLatestVersion}"
        + " AND audit_info = #{oldFunctionMeta.auditInfo}"
        + " AND deleted_at = 0";
  }
}
