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

import static org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper.VERSION_TABLE_NAME;

import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.ibatis.annotations.Param;

public class FilesetVersionBaseSQLProvider {
  public String insertFilesetVersion(@Param("filesetVersion") FilesetVersionPO filesetVersionPO) {
    return "INSERT INTO "
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
        + " )";
  }

  public String insertFilesetVersionOnDuplicateKeyUpdate(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO) {
    return "INSERT INTO "
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
        + " deleted_at = #{filesetVersion.deletedAt}";
  }

  public String softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId) {
    return "UPDATE "
        + VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE fileset_id = #{filesetId} AND deleted_at = 0";
  }

  public String deleteFilesetVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + VERSION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String selectFilesetVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return "SELECT fileset_id as filesetId,"
        + " Max(version) as version"
        + " FROM "
        + VERSION_TABLE_NAME
        + " WHERE version > #{versionRetentionCount} AND deleted_at = 0"
        + " GROUP BY fileset_id";
  }

  public String softDeleteFilesetVersionsByRetentionLine(
      @Param("filesetId") Long filesetId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + VERSION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE fileset_id = #{filesetId} AND version <= #{versionRetentionLine} AND deleted_at = 0 LIMIT #{limit}";
  }
}
