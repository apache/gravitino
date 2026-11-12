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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewVersionInfoMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.mapper.provider.base.ViewVersionInfoBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ViewVersionInfoPostgreSQLProvider extends ViewVersionInfoBaseSQLProvider {

  @Override
  public String softDeleteViewVersionsByViewId(@Param("viewId") Long viewId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE view_id = #{viewId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteViewVersionsBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE view_id IN (SELECT view_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ")) AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String softDeleteViewVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE view_id IN (SELECT view_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " WHERE catalog_id = #{catalogId}) AND deleted_at = 0";
  }

  @Override
  public String softDeleteViewVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ViewVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.POSTGRESQL
        + " WHERE view_id IN (SELECT view_id FROM "
        + ViewMetaMapper.TABLE_NAME
        + " WHERE metalake_id = #{metalakeId}) AND deleted_at = 0";
  }

  @Override
  public String deleteViewVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ViewVersionInfoMapper.TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + ViewVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
