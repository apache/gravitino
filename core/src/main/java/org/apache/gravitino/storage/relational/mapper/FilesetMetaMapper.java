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
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for fileset meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface FilesetMetaMapper {
  String META_TABLE_NAME = "fileset_meta";

  String VERSION_TABLE_NAME = "fileset_version_info";

  @Select(
      "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
          + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
          + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
          + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
          + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location,"
          + " vi.deleted_at as version_deleted_at"
          + " FROM "
          + META_TABLE_NAME
          + " fm INNER JOIN "
          + VERSION_TABLE_NAME
          + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
          + " WHERE fm.schema_id = #{schemaId} AND fm.deleted_at = 0 AND vi.deleted_at = 0")
  @Results({
    @Result(property = "filesetId", column = "fileset_id"),
    @Result(property = "filesetName", column = "fileset_name"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "catalogId", column = "catalog_id"),
    @Result(property = "schemaId", column = "schema_id"),
    @Result(property = "type", column = "type"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "currentVersion", column = "current_version"),
    @Result(property = "lastVersion", column = "last_version"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(property = "filesetVersionPO.id", column = "id"),
    @Result(property = "filesetVersionPO.metalakeId", column = "version_metalake_id"),
    @Result(property = "filesetVersionPO.catalogId", column = "version_catalog_id"),
    @Result(property = "filesetVersionPO.schemaId", column = "version_schema_id"),
    @Result(property = "filesetVersionPO.filesetId", column = "version_fileset_id"),
    @Result(property = "filesetVersionPO.version", column = "version"),
    @Result(property = "filesetVersionPO.filesetComment", column = "fileset_comment"),
    @Result(property = "filesetVersionPO.properties", column = "properties"),
    @Result(property = "filesetVersionPO.storageLocation", column = "storage_location"),
    @Result(property = "filesetVersionPO.deletedAt", column = "version_deleted_at")
  })
  List<FilesetPO> listFilesetPOsBySchemaId(@Param("schemaId") Long schemaId);

  @Select(
      "SELECT fileset_id as filesetId FROM "
          + META_TABLE_NAME
          + " WHERE schema_id = #{schemaId} AND fileset_name = #{filesetName}"
          + " AND deleted_at = 0")
  Long selectFilesetIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name);

  @Select(
      "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
          + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
          + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
          + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
          + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location,"
          + " vi.deleted_at as version_deleted_at"
          + " FROM "
          + META_TABLE_NAME
          + " fm INNER JOIN "
          + VERSION_TABLE_NAME
          + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
          + " WHERE fm.schema_id = #{schemaId} AND fm.fileset_name = #{filesetName}"
          + " AND fm.deleted_at = 0 AND vi.deleted_at = 0")
  @Results({
    @Result(property = "filesetId", column = "fileset_id"),
    @Result(property = "filesetName", column = "fileset_name"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "catalogId", column = "catalog_id"),
    @Result(property = "schemaId", column = "schema_id"),
    @Result(property = "type", column = "type"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "currentVersion", column = "current_version"),
    @Result(property = "lastVersion", column = "last_version"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(property = "filesetVersionPO.id", column = "id"),
    @Result(property = "filesetVersionPO.metalakeId", column = "version_metalake_id"),
    @Result(property = "filesetVersionPO.catalogId", column = "version_catalog_id"),
    @Result(property = "filesetVersionPO.schemaId", column = "version_schema_id"),
    @Result(property = "filesetVersionPO.filesetId", column = "version_fileset_id"),
    @Result(property = "filesetVersionPO.version", column = "version"),
    @Result(property = "filesetVersionPO.filesetComment", column = "fileset_comment"),
    @Result(property = "filesetVersionPO.properties", column = "properties"),
    @Result(property = "filesetVersionPO.storageLocation", column = "storage_location"),
    @Result(property = "filesetVersionPO.deletedAt", column = "version_deleted_at")
  })
  FilesetPO selectFilesetMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name);

  @Select(
      "SELECT fm.fileset_id, fm.fileset_name, fm.metalake_id, fm.catalog_id, fm.schema_id,"
          + " fm.type, fm.audit_info, fm.current_version, fm.last_version, fm.deleted_at,"
          + " vi.id, vi.metalake_id as version_metalake_id, vi.catalog_id as version_catalog_id,"
          + " vi.schema_id as version_schema_id, vi.fileset_id as version_fileset_id,"
          + " vi.version, vi.fileset_comment, vi.properties, vi.storage_location,"
          + " vi.deleted_at as version_deleted_at"
          + " FROM "
          + META_TABLE_NAME
          + " fm INNER JOIN "
          + VERSION_TABLE_NAME
          + " vi ON fm.fileset_id = vi.fileset_id AND fm.current_version = vi.version"
          + " WHERE fm.fileset_id = #{filesetId}"
          + " AND fm.deleted_at = 0 AND vi.deleted_at = 0")
  @Results({
    @Result(property = "filesetId", column = "fileset_id"),
    @Result(property = "filesetName", column = "fileset_name"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "catalogId", column = "catalog_id"),
    @Result(property = "schemaId", column = "schema_id"),
    @Result(property = "type", column = "type"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "currentVersion", column = "current_version"),
    @Result(property = "lastVersion", column = "last_version"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(property = "filesetVersionPO.id", column = "id"),
    @Result(property = "filesetVersionPO.metalakeId", column = "version_metalake_id"),
    @Result(property = "filesetVersionPO.catalogId", column = "version_catalog_id"),
    @Result(property = "filesetVersionPO.schemaId", column = "version_schema_id"),
    @Result(property = "filesetVersionPO.filesetId", column = "version_fileset_id"),
    @Result(property = "filesetVersionPO.version", column = "version"),
    @Result(property = "filesetVersionPO.filesetComment", column = "fileset_comment"),
    @Result(property = "filesetVersionPO.properties", column = "properties"),
    @Result(property = "filesetVersionPO.storageLocation", column = "storage_location"),
    @Result(property = "filesetVersionPO.deletedAt", column = "version_deleted_at")
  })
  FilesetPO selectFilesetMetaById(@Param("filesetId") Long filesetId);

  @Insert(
      "INSERT INTO "
          + META_TABLE_NAME
          + "(fileset_id, fileset_name, metalake_id,"
          + " catalog_id, schema_id, type, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
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
          + " )")
  void insertFilesetMeta(@Param("filesetMeta") FilesetPO filesetPO);

  @Insert(
      "INSERT INTO "
          + META_TABLE_NAME
          + "(fileset_id, fileset_name, metalake_id,"
          + " catalog_id, schema_id, type, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
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
          + " current_version = #{filesetMeta.currentVersion},"
          + " last_version = #{filesetMeta.lastVersion},"
          + " deleted_at = #{filesetMeta.deletedAt}")
  void insertFilesetMetaOnDuplicateKeyUpdate(@Param("filesetMeta") FilesetPO filesetPO);

  @Update(
      "UPDATE "
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
          + " AND fileset_name = #{oldFilesetMeta.filesetName}"
          + " AND metalake_id = #{oldFilesetMeta.metalakeId}"
          + " AND catalog_id = #{oldFilesetMeta.catalogId}"
          + " AND schema_id = #{oldFilesetMeta.schemaId}"
          + " AND type = #{oldFilesetMeta.type}"
          + " AND audit_info = #{oldFilesetMeta.auditInfo}"
          + " AND current_version = #{oldFilesetMeta.currentVersion}"
          + " AND last_version = #{oldFilesetMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateFilesetMeta(
      @Param("newFilesetMeta") FilesetPO newFilesetPO,
      @Param("oldFilesetMeta") FilesetPO oldFilesetPO);

  @Update(
      "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteFilesetMetasBySchemaId(@Param("schemaId") Long schemaId);

  @Update(
      "UPDATE "
          + META_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE fileset_id = #{filesetId} AND deleted_at = 0")
  Integer softDeleteFilesetMetasByFilesetId(@Param("filesetId") Long filesetId);

  @Delete(
      "DELETE FROM "
          + META_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
