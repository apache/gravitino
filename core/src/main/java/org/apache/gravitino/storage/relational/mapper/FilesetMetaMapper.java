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
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

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
  @SelectProvider(type = FilesetMetaSQLProviderFactory.class, method = "listFilesetPOsBySchemaId")
  List<FilesetPO> listFilesetPOsBySchemaId(@Param("schemaId") Long schemaId);

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
  @SelectProvider(type = FilesetMetaSQLProviderFactory.class, method = "listFilesetPOsByFilesetIds")
  List<FilesetPO> listFilesetPOsByFilesetIds(@Param("filesetIds") List<Long> filesetIds);

  @SelectProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "selectFilesetIdBySchemaIdAndName")
  Long selectFilesetIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name);

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
  @SelectProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "selectFilesetMetaBySchemaIdAndName")
  FilesetPO selectFilesetMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name);

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
  @SelectProvider(type = FilesetMetaSQLProviderFactory.class, method = "selectFilesetMetaById")
  FilesetPO selectFilesetMetaById(@Param("filesetId") Long filesetId);

  @InsertProvider(type = FilesetMetaSQLProviderFactory.class, method = "insertFilesetMeta")
  void insertFilesetMeta(@Param("filesetMeta") FilesetPO filesetPO);

  @InsertProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "insertFilesetMetaOnDuplicateKeyUpdate")
  void insertFilesetMetaOnDuplicateKeyUpdate(@Param("filesetMeta") FilesetPO filesetPO);

  @UpdateProvider(type = FilesetMetaSQLProviderFactory.class, method = "updateFilesetMeta")
  Integer updateFilesetMeta(
      @Param("newFilesetMeta") FilesetPO newFilesetPO,
      @Param("oldFilesetMeta") FilesetPO oldFilesetPO);

  @UpdateProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "softDeleteFilesetMetasByMetalakeId")
  Integer softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "softDeleteFilesetMetasByCatalogId")
  Integer softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "softDeleteFilesetMetasBySchemaId")
  Integer softDeleteFilesetMetasBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "softDeleteFilesetMetasByFilesetId")
  Integer softDeleteFilesetMetasByFilesetId(@Param("filesetId") Long filesetId);

  @DeleteProvider(
      type = FilesetMetaSQLProviderFactory.class,
      method = "deleteFilesetMetasByLegacyTimeline")
  Integer deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
