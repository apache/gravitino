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

import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.ViewVersionInfoPO;
import org.apache.ibatis.annotations.Param;

public class ViewVersionInfoBaseSQLProvider {

  public String insertViewVersionInfo(
      @Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO) {
    return "INSERT INTO "
        + ViewVersionInfoMapper.TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, view_id, version,"
        + " view_comment, columns, properties, default_catalog, default_schema, representations,"
        + " audit_info, deleted_at)"
        + " VALUES (#{viewVersionInfo.metalakeId}, #{viewVersionInfo.catalogId},"
        + " #{viewVersionInfo.schemaId}, #{viewVersionInfo.viewId},"
        + " #{viewVersionInfo.version}, #{viewVersionInfo.viewComment},"
        + " #{viewVersionInfo.columns}, #{viewVersionInfo.properties},"
        + " #{viewVersionInfo.defaultCatalog}, #{viewVersionInfo.defaultSchema},"
        + " #{viewVersionInfo.representations},"
        + " #{viewVersionInfo.auditInfo}, #{viewVersionInfo.deletedAt})";
  }

  public String insertViewVersionInfoOnDuplicateKeyUpdate(
      @Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO) {
    return "INSERT INTO "
        + ViewVersionInfoMapper.TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, view_id, version,"
        + " view_comment, columns, properties, default_catalog, default_schema, representations,"
        + " audit_info, deleted_at)"
        + " VALUES (#{viewVersionInfo.metalakeId}, #{viewVersionInfo.catalogId},"
        + " #{viewVersionInfo.schemaId}, #{viewVersionInfo.viewId},"
        + " #{viewVersionInfo.version}, #{viewVersionInfo.viewComment},"
        + " #{viewVersionInfo.columns}, #{viewVersionInfo.properties},"
        + " #{viewVersionInfo.defaultCatalog}, #{viewVersionInfo.defaultSchema},"
        + " #{viewVersionInfo.representations},"
        + " #{viewVersionInfo.auditInfo}, #{viewVersionInfo.deletedAt})"
        + " ON DUPLICATE KEY UPDATE"
        + " view_comment = #{viewVersionInfo.viewComment},"
        + " columns = #{viewVersionInfo.columns},"
        + " properties = #{viewVersionInfo.properties},"
        + " default_catalog = #{viewVersionInfo.defaultCatalog},"
        + " default_schema = #{viewVersionInfo.defaultSchema},"
        + " representations = #{viewVersionInfo.representations},"
        + " audit_info = #{viewVersionInfo.auditInfo},"
        + " deleted_at = #{viewVersionInfo.deletedAt}";
  }

  public String selectViewVersionInfoByViewIdAndVersion(
      @Param("viewId") Long viewId, @Param("version") Integer version) {
    return "SELECT id as id, metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId, view_id as viewId, version as version,"
        + " view_comment as viewComment, columns as columns, properties as properties,"
        + " default_catalog as defaultCatalog, default_schema as defaultSchema,"
        + " representations as representations,"
        + " audit_info as auditInfo, deleted_at as deletedAt FROM "
        + ViewVersionInfoMapper.TABLE_NAME
        + " WHERE view_id = #{viewId} AND version = #{version} AND deleted_at = 0";
  }

  public String softDeleteViewVersionsByViewId(@Param("viewId") Long viewId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE view_id = #{viewId} AND deleted_at = 0";
  }

  public String softDeleteViewVersionsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteViewVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteViewVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String deleteViewVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ViewVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
