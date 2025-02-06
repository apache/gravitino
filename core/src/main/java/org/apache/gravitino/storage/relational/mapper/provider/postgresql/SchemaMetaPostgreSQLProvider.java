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

import org.apache.gravitino.storage.relational.mapper.provider.base.SchemaMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaPostgreSQLProvider extends SchemaMetaBaseSQLProvider {
  @Override
  public String insertSchemaMetaOnDuplicateKeyUpdate(SchemaPO schemaPO) {
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
        + " ON CONFLICT(schema_id) DO UPDATE SET "
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

  @Override
  public String softDeleteSchemaMetasBySchemaId(Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteSchemaMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteSchemaMetasByCatalogId(Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE schema_id IN (SELECT schema_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
