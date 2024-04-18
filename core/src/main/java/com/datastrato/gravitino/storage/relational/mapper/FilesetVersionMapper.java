/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
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
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId);

  @Update(
      "UPDATE "
          + VERSION_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE fileset_id = #{filesetId} AND deleted_at = 0")
  Integer softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId);

  @Delete(
      "DELETE FROM "
          + VERSION_TABLE_NAME
          + " WHERE deleted_at != 0 AND deleted_at < #{legacyTimeLine} LIMIT #{limit}")
  Integer deleteFilesetVersionsByLegacyTimeLine(
      @Param("legacyTimeLine") Long legacyTimeLine, @Param("limit") int limit);

  @Select(
      "SELECT fileset_id as filesetId,"
          + " Max(version) as version, deleted_at as deletedAt,"
          + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId,"
          + " storage_location as storageLocation"
          + " FROM "
          + VERSION_TABLE_NAME
          + " WHERE version > #{versionRetentionCount} AND deleted_at = 0"
          + " GROUP BY fileset_id")
  List<FilesetVersionPO> selectFilesetVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  @Delete(
      "DELETE FROM "
          + VERSION_TABLE_NAME
          + " WHERE fileset_id = #{filesetId} AND version <= #{versionRetentionLine} AND deleted_at = 0 LIMIT #{limit}")
  Integer deleteFilesetVersionsByRetentionLine(
      @Param("filesetId") Long filesetId,
      @Param("versionRetentionLine") Long versionRetentionLine,
      @Param("limit") int limit);
}
