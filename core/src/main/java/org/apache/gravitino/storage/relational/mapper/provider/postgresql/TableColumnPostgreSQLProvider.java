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

import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.TableColumnBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class TableColumnPostgreSQLProvider extends TableColumnBaseSQLProvider {

  @Override
  public String softDeleteColumnsByTableId(@Param("tableId") Long tableId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE table_id = #{tableId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteColumnsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteColumnsByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteColumnsBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String deleteColumnPOsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
