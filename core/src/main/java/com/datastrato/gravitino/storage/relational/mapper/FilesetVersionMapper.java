/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for fileset version info operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface FilesetVersionMapper {
  String VERSION_TABLE_NAME = "fileset_version_info";

  @Insert(
      "INSERT INTO "
          + VERSION_TABLE_NAME
          + "(metalake_id, catalog_id, schema_id, fileset_id,"
          + " version, fileset_comment, properties, storage_location,"
          + " deleted_at)"
          + " VALUES("
          + " #{filesetVersion.metalakeId},"
          + " #{filesetVersion.catalogId},"
          + " #{filesetVersion.schemaId},"
          + " #{filesetVersion.filesetId},"
          + " #{filesetVersion.version},"
          + " #{filesetVersion.filesetComment},"
          + " #{filesetVersion.properties},"
          + " #{filesetVersion.storageLocation},"
          + " #{filesetVersion.deletedAt}"
          + " )")
  void insertFilesetVersion(@Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @Insert(
      "INSERT INTO "
          + VERSION_TABLE_NAME
          + "(metalake_id, catalog_id, schema_id, fileset_id,"
          + " version, fileset_comment, properties, storage_location,"
          + " deleted_at)"
          + " VALUES("
          + " #{filesetVersion.metalakeId},"
          + " #{filesetVersion.catalogId},"
          + " #{filesetVersion.schemaId},"
          + " #{filesetVersion.filesetId},"
          + " #{filesetVersion.version},"
          + " #{filesetVersion.filesetComment},"
          + " #{filesetVersion.properties},"
          + " #{filesetVersion.storageLocation},"
          + " #{filesetVersion.deletedAt}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " metalake_id = #{filesetVersion.metalakeId},"
          + " catalog_id = #{filesetVersion.catalogId},"
          + " schema_id = #{filesetVersion.schemaId},"
          + " fileset_id = #{filesetVersion.filesetId},"
          + " version = #{filesetVersion.version},"
          + " fileset_comment = #{filesetVersion.filesetComment},"
          + " properties = #{filesetVersion.properties},"
          + " storage_location = #{filesetVersion.storageLocation},"
          + " deleted_at = #{filesetVersion.deletedAt}")
  void insertFilesetVersionOnDuplicateKeyUpdate(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP()"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP()"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP()"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP()"
          + " WHERE fileset_id = #{filesetId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId);
}
