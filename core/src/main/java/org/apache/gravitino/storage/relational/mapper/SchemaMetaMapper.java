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

package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for schema meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface SchemaMetaMapper {
  String TABLE_NAME = "schema_meta";

  @Select(
      "SELECT schema_id as schemaId, schema_name as schemaName,"
          + " metalake_id as metalakeId, catalog_id as catalogId,"
          + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  List<SchemaPO> listSchemaPOsByCatalogId(@Param("catalogId") Long catalogId);

  @Select(
      "SELECT schema_id as schemaId FROM "
          + TABLE_NAME
          + " WHERE catalog_id = #{catalogId} AND schema_name = #{schemaName}"
          + " AND deleted_at = 0")
  Long selectSchemaIdByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name);

  @Select(
      "SELECT schema_id as schemaId, schema_name as schemaName,"
          + " metalake_id as metalakeId, catalog_id as catalogId,"
          + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE catalog_id = #{catalogId} AND schema_name = #{schemaName} AND deleted_at = 0")
  SchemaPO selectSchemaMetaByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name);

  @Select(
      "SELECT schema_id as schemaId, schema_name as schemaName,"
          + " metalake_id as metalakeId, catalog_id as catalogId,"
          + " schema_comment as schemaComment, properties, audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  SchemaPO selectSchemaMetaById(@Param("schemaId") Long schemaId);

  @Insert(
      "INSERT INTO "
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
          + " )")
  void insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO);

  @Insert(
      "INSERT INTO "
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
          + " deleted_at = #{schemaMeta.deletedAt}")
  void insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO);

  @Update(
      "UPDATE "
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
          + " AND deleted_at = 0")
  Integer updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId);

  @Delete(
      "DELETE FROM "
          + TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
