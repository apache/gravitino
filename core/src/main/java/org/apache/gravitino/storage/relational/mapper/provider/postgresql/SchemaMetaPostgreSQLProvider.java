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

import static org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.SchemaMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaPostgreSQLProvider extends SchemaMetaBaseSQLProvider {
  @Override
  public String selectSchemaMetaByIdForShare(Long schemaId) {
    return selectSchemaMetaById(schemaId) + " FOR SHARE";
  }

  @Override
  public String insertSchemaMetaOnDuplicateKeyUpdate(SchemaPO schemaPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (schema_id, schema_name, metalake_id,"
        + " catalog_id, schema_comment, properties, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
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
        + " ON CONFLICT(schema_id) DO UPDATE SET"
        + " schema_name = #{schemaMeta.schemaName},"
        + " metalake_id = #{schemaMeta.metalakeId},"
        + " catalog_id = #{schemaMeta.catalogId},"
        + " schema_comment = #{schemaMeta.schemaComment},"
        + " properties = #{schemaMeta.properties},"
        + " audit_info = #{schemaMeta.auditInfo},"
        // Move the version forward instead of writing the initial version again. Resetting it
        // would let a slow alter or drop that still holds an older version pass its own version
        // check later on. The column has to be written as <table>.<column> here: on this side of
        // ON CONFLICT a bare name could mean either the stored row or the rejected one, and
        // PostgreSQL refuses it as ambiguous.
        + " current_version = "
        + TABLE_NAME
        + ".current_version + 1,"
        + " last_version = "
        + TABLE_NAME
        + ".current_version + 1,"
        + " deleted_at = #{schemaMeta.deletedAt}";
  }

  @Override
  public String batchInsertSchemaMetaOnDuplicateKeyUpdate(
      @Param("schemaMetas") List<SchemaPO> schemaMetas) {
    return "<script>"
        + "INSERT INTO "
        + TABLE_NAME
        + " (schema_id, schema_name, metalake_id, catalog_id, schema_comment,"
        + " properties, audit_info, current_version, last_version, deleted_at) VALUES "
        + "<foreach collection='schemaMetas' item='po' separator=','>"
        + "(#{po.schemaId}, #{po.schemaName}, #{po.metalakeId}, #{po.catalogId},"
        + " #{po.schemaComment}, #{po.properties}, #{po.auditInfo},"
        + " #{po.currentVersion}, #{po.lastVersion}, #{po.deletedAt})"
        + "</foreach>"
        + " ON CONFLICT(schema_id) DO UPDATE SET"
        + " schema_name = EXCLUDED.schema_name,"
        + " metalake_id = EXCLUDED.metalake_id,"
        + " catalog_id = EXCLUDED.catalog_id,"
        + " schema_comment = EXCLUDED.schema_comment,"
        + " properties = EXCLUDED.properties,"
        + " audit_info = EXCLUDED.audit_info,"
        // Move the version forward instead of writing the initial version again. Resetting it
        // would let a slow alter or drop that still holds an older version pass its own version
        // check later on. The column has to be written as <table>.<column> here: on this side of
        // ON CONFLICT a bare name could mean either the stored row or the rejected one, and
        // PostgreSQL refuses it as ambiguous.
        + " current_version = "
        + TABLE_NAME
        + ".current_version + 1,"
        + " last_version = "
        + TABLE_NAME
        + ".current_version + 1,"
        + " deleted_at = EXCLUDED.deleted_at"
        + "</script>";
  }

  @Override
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
        + " AND current_version = #{oldSchemaMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteSchemaMetasBySchemaIds(List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String softDeleteSchemaMetaBySchemaIdAndVersion(Long schemaId, Long currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id = #{schemaId}"
        + " AND current_version = #{currentVersion} AND deleted_at = 0";
  }

  /** {@inheritDoc} */
  @Override
  public String softDeleteSchemaMetasWithVersion(List<SchemaPO> schemaPOs) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE deleted_at = 0 AND "
        + "<foreach collection='schemaMetas' item='item' separator=' OR ' open='(' close=')'>"
        + "(schema_id = #{item.schemaId} AND current_version = #{item.currentVersion})"
        + "</foreach>"
        + "</script>";
  }

  @Override
  public String deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE schema_id IN (SELECT schema_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
