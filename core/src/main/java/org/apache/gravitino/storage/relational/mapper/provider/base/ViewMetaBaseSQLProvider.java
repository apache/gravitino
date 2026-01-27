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

import static org.apache.gravitino.storage.relational.mapper.ViewMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.ibatis.annotations.Param;

public class ViewMetaBaseSQLProvider {

  public String listViewPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT view_id as viewId, view_name as viewName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String selectViewIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name) {
    return "SELECT view_id as viewId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND view_name = #{viewName}"
        + " AND deleted_at = 0";
  }

  public String selectViewMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name) {
    return "SELECT view_id as viewId, view_name as viewName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND view_name = #{viewName} AND deleted_at = 0";
  }

  public String insertViewMeta(@Param("viewMeta") ViewPO viewPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (view_id, view_name, metalake_id,"
        + " catalog_id, schema_id,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{viewMeta.viewId},"
        + " #{viewMeta.viewName},"
        + " #{viewMeta.metalakeId},"
        + " #{viewMeta.catalogId},"
        + " #{viewMeta.schemaId},"
        + " #{viewMeta.currentVersion},"
        + " #{viewMeta.lastVersion},"
        + " #{viewMeta.deletedAt}"
        + " )";
  }

  public String insertViewMetaOnDuplicateKeyUpdate(@Param("viewMeta") ViewPO viewPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (view_id, view_name, metalake_id,"
        + " catalog_id, schema_id,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{viewMeta.viewId},"
        + " #{viewMeta.viewName},"
        + " #{viewMeta.metalakeId},"
        + " #{viewMeta.catalogId},"
        + " #{viewMeta.schemaId},"
        + " #{viewMeta.currentVersion},"
        + " #{viewMeta.lastVersion},"
        + " #{viewMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " view_name = #{viewMeta.viewName},"
        + " metalake_id = #{viewMeta.metalakeId},"
        + " catalog_id = #{viewMeta.catalogId},"
        + " schema_id = #{viewMeta.schemaId},"
        + " current_version = #{viewMeta.currentVersion},"
        + " last_version = #{viewMeta.lastVersion},"
        + " deleted_at = #{viewMeta.deletedAt}";
  }

  public String updateViewMeta(
      @Param("newViewMeta") ViewPO newViewPO, @Param("oldViewMeta") ViewPO oldViewPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET view_name = #{newViewMeta.viewName}, "
        + " current_version = #{newViewMeta.currentVersion}, "
        + " last_version = #{newViewMeta.lastVersion}, "
        + " deleted_at = #{newViewMeta.deletedAt} "
        + " WHERE view_id = #{oldViewMeta.viewId} "
        + " AND current_version = #{oldViewMeta.currentVersion} "
        + " AND deleted_at = 0";
  }

  public String listViewPOsByViewIds(@Param("viewIds") List<Long> viewIds) {
    return "<script>"
        + "SELECT view_id as viewId, view_name as viewName, "
        + " metalake_id as metalakeId, catalog_id as catalogId, schema_id as schemaId, "
        + " current_version as currentVersion, last_version as lastVersion, deleted_at as deletedAt "
        + " FROM "
        + TABLE_NAME
        + " WHERE view_id IN "
        + "<foreach item='viewId' index='index' collection='viewIds' open='(' separator=',' close=')'>"
        + "#{viewId}"
        + "</foreach>"
        + " AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteViewMetasByViewId(@Param("viewId") Long viewId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE view_id = #{viewId} AND deleted_at = 0";
  }

  public String softDeleteViewMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteViewMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteViewMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String deleteViewMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
