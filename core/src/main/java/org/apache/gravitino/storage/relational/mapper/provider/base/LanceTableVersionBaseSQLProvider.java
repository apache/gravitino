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

import static org.apache.gravitino.storage.relational.mapper.LanceTableVersionMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.LanceTableVersionPO;
import org.apache.ibatis.annotations.Param;

public class LanceTableVersionBaseSQLProvider {

  public String insertTableVersion(@Param("tableVersion") LanceTableVersionPO tableVersionPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, table_id, version,"
        + " manifest_path, manifest_size, e_tag, naming_scheme, metadata_json,"
        + " context_json, created_at, created_by, request_id, deleted_at)"
        + " VALUES ("
        + " #{tableVersion.metalakeId}, #{tableVersion.catalogId}, #{tableVersion.schemaId},"
        + " #{tableVersion.tableId}, #{tableVersion.version}, #{tableVersion.manifestPath},"
        + " #{tableVersion.manifestSize}, #{tableVersion.ETag}, #{tableVersion.namingScheme},"
        + " #{tableVersion.metadata}, #{tableVersion.context}, #{tableVersion.createdAt},"
        + " #{tableVersion.createdBy}, #{tableVersion.requestId}, #{tableVersion.deletedAt})";
  }

  public String batchInsertTableVersions(
      @Param("tableVersions") List<LanceTableVersionPO> tableVersionPOs) {
    return "<script>"
        + "INSERT INTO "
        + TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, table_id, version,"
        + " manifest_path, manifest_size, e_tag, naming_scheme, metadata_json,"
        + " context_json, created_at, created_by, request_id, deleted_at)"
        + " VALUES "
        + "<foreach collection='tableVersions' item='versionInfo' separator=','>"
        + " (#{versionInfo.metalakeId}, #{versionInfo.catalogId}, #{versionInfo.schemaId},"
        + " #{versionInfo.tableId}, #{versionInfo.version}, #{versionInfo.manifestPath},"
        + " #{versionInfo.manifestSize}, #{versionInfo.ETag}, #{versionInfo.namingScheme},"
        + " #{versionInfo.metadata}, #{versionInfo.context}, #{versionInfo.createdAt},"
        + " #{versionInfo.createdBy}, #{versionInfo.requestId}, #{versionInfo.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String selectTableVersion(@Param("tableId") Long tableId, @Param("version") Long version) {
    return "SELECT id, metalake_id AS metalakeId, catalog_id AS catalogId, schema_id AS schemaId,"
        + " table_id AS tableId, version, manifest_path AS manifestPath,"
        + " manifest_size AS manifestSize, e_tag AS ETag, naming_scheme AS namingScheme,"
        + " metadata_json AS metadata, context_json AS context, created_at AS createdAt,"
        + " created_by AS createdBy, request_id AS requestId, deleted_at AS deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND version = #{version} AND deleted_at = 0";
  }

  public String listTableVersions(
      @Param("tableId") Long tableId,
      @Param("limit") Integer limit,
      @Param("startVersionExclusive") Long startVersionExclusive,
      @Param("descending") boolean descending) {
    return "<script>"
        + "SELECT id, metalake_id AS metalakeId, catalog_id AS catalogId, schema_id AS schemaId,"
        + " table_id AS tableId, version, manifest_path AS manifestPath,"
        + " manifest_size AS manifestSize, e_tag AS ETag, naming_scheme AS namingScheme,"
        + " metadata_json AS metadata, context_json AS context, created_at AS createdAt,"
        + " created_by AS createdBy, request_id AS requestId, deleted_at AS deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND deleted_at = 0"
        + " <if test='startVersionExclusive != null'>"
        + " <choose>"
        + " <when test='descending'> AND version &lt; #{startVersionExclusive}</when>"
        + " <otherwise> AND version &gt; #{startVersionExclusive}</otherwise>"
        + " </choose>"
        + " </if>"
        + " <choose>"
        + " <when test='descending'> ORDER BY version DESC</when>"
        + " <otherwise> ORDER BY version ASC</otherwise>"
        + " </choose>"
        + " LIMIT #{limit}"
        + "</script>";
  }

  public String softDeleteTableVersions(
      @Param("tableId") Long tableId,
      @Param("versions") List<Long> versions,
      @Param("deletedAt") Long deletedAt) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = #{deletedAt}"
        + " WHERE table_id = #{tableId} AND deleted_at = 0 AND version IN "
        + "<foreach collection='versions' item='version' open='(' separator=',' close=')'>"
        + "#{version}"
        + "</foreach>"
        + "</script>";
  }
}
