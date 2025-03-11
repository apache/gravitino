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

import static org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaBaseSQLProvider {
  public String listSchemaPOsByCatalogId(@Param("catalogId") Long catalogId) {
    return "SELECT schema_id as schemaId, schema_name as schemaName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String listSchemaPOsBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "SELECT schema_id as schemaId, schema_name as schemaName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id in ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") "
        + " AND deleted_at = 0"
        + "</script>";
  }

  public String selectSchemaIdByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return "SELECT schema_id as schemaId FROM "
        + TABLE_NAME
        + " WHERE catalog_id = #{catalogId} AND schema_name = #{schemaName}"
        + " AND deleted_at = 0";
  }

  public String selectSchemaMetaByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return "SELECT schema_id as schemaId, schema_name as schemaName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE catalog_id = #{catalogId} AND schema_name = #{schemaName} AND deleted_at = 0";
  }

  public String selectSchemaMetaById(@Param("schemaId") Long schemaId) {
    return "SELECT schema_id as schemaId, schema_name as schemaName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + "(schema_id, schema_name, metalake_id,"
        + " catalog_id, schema_comment, properties, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{schemaMeta.schemaId},"
        + " #{schemaMeta.schemaName},"
        + " #{schemaMeta.metalakeId},"
        + " #{schemaMeta.catalogId},"
        + " #{schemaMeta.schemaComment},"
        + " #{schemaMeta.properties},"
        + " #{schemaMeta.auditInfo},"
        + " #{schemaMeta.currentVersion},"
        + " #{schemaMeta.lastVersion},"
        + " #{schemaMeta.deletedAt}"
        + " )";
  }

  public String insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + "(schema_id, schema_name, metalake_id,"
        + " catalog_id, schema_comment, properties, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{schemaMeta.schemaId},"
        + " #{schemaMeta.schemaName},"
        + " #{schemaMeta.metalakeId},"
        + " #{schemaMeta.catalogId},"
        + " #{schemaMeta.schemaComment},"
        + " #{schemaMeta.properties},"
        + " #{schemaMeta.auditInfo},"
        + " #{schemaMeta.currentVersion},"
        + " #{schemaMeta.lastVersion},"
        + " #{schemaMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " schema_name = #{schemaMeta.schemaName},"
        + " metalake_id = #{schemaMeta.metalakeId},"
        + " catalog_id = #{schemaMeta.catalogId},"
        + " schema_comment = #{schemaMeta.schemaComment},"
        + " properties = #{schemaMeta.properties},"
        + " audit_info = #{schemaMeta.auditInfo},"
        + " current_version = #{schemaMeta.currentVersion},"
        + " last_version = #{schemaMeta.lastVersion},"
        + " deleted_at = #{schemaMeta.deletedAt}";
  }

  public String updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET schema_name = #{newSchemaMeta.schemaName},"
        + " metalake_id = #{newSchemaMeta.metalakeId},"
        + " catalog_id = #{newSchemaMeta.catalogId},"
        + " schema_comment = #{newSchemaMeta.schemaComment},"
        + " properties = #{newSchemaMeta.properties},"
        + " audit_info = #{newSchemaMeta.auditInfo},"
        + " current_version = #{newSchemaMeta.currentVersion},"
        + " last_version = #{newSchemaMeta.lastVersion},"
        + " deleted_at = #{newSchemaMeta.deletedAt}"
        + " WHERE schema_id = #{oldSchemaMeta.schemaId}"
        + " AND schema_name = #{oldSchemaMeta.schemaName}"
        + " AND metalake_id = #{oldSchemaMeta.metalakeId}"
        + " AND catalog_id = #{oldSchemaMeta.catalogId}"
        + " AND (schema_comment IS NULL OR schema_comment = #{oldSchemaMeta.schemaComment})"
        + " AND properties = #{oldSchemaMeta.properties}"
        + " AND audit_info = #{oldSchemaMeta.auditInfo}"
        + " AND current_version = #{oldSchemaMeta.currentVersion}"
        + " AND last_version = #{oldSchemaMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
