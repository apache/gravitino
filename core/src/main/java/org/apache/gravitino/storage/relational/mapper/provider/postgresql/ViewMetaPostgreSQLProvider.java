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

import static org.apache.gravitino.storage.relational.mapper.ViewMetaMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.ViewMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.ibatis.annotations.Param;

public class ViewMetaPostgreSQLProvider extends ViewMetaBaseSQLProvider {

  @Override
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
        + " ON CONFLICT (view_id) DO UPDATE SET"
        + " view_name = #{viewMeta.viewName},"
        + " metalake_id = #{viewMeta.metalakeId},"
        + " catalog_id = #{viewMeta.catalogId},"
        + " schema_id = #{viewMeta.schemaId},"
        + " current_version = #{viewMeta.currentVersion},"
        + " last_version = #{viewMeta.lastVersion},"
        + " deleted_at = #{viewMeta.deletedAt}";
  }

  @Override
  public String softDeleteViewMetasByViewId(@Param("viewId") Long viewId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE view_id = #{viewId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteViewMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteViewMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteViewMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String deleteViewMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE view_id IN (SELECT view_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
