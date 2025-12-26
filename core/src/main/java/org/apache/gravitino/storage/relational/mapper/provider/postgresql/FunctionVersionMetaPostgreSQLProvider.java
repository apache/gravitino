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

import org.apache.gravitino.storage.relational.mapper.FunctionVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.FunctionVersionMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.FunctionVersionPO;
import org.apache.ibatis.annotations.Param;

public class FunctionVersionMetaPostgreSQLProvider extends FunctionVersionMetaBaseSQLProvider {

  @Override
  public String insertFunctionVersionMetaOnDuplicateKeyUpdate(
      @Param("functionVersionMeta") FunctionVersionPO functionVersionPO) {
    return "INSERT INTO "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, function_id, version,"
        + " function_comment, definitions, audit_info, deleted_at)"
        + " VALUES (#{functionVersionMeta.metalakeId}, #{functionVersionMeta.catalogId},"
        + " #{functionVersionMeta.schemaId}, #{functionVersionMeta.functionId},"
        + " #{functionVersionMeta.functionVersion}, #{functionVersionMeta.functionComment},"
        + " #{functionVersionMeta.definitions}, #{functionVersionMeta.auditInfo},"
        + " #{functionVersionMeta.deletedAt})"
        + " ON CONFLICT (function_id, version, deleted_at) DO UPDATE SET"
        + " function_comment = #{functionVersionMeta.functionComment},"
        + " definitions = #{functionVersionMeta.definitions},"
        + " audit_info = #{functionVersionMeta.auditInfo},"
        + " deleted_at = #{functionVersionMeta.deletedAt}";
  }

  @Override
  public String softDeleteFunctionVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteFunctionVersionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteFunctionVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String deleteFunctionVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String softDeleteFunctionVersionsByRetentionLine(
      @Param("functionId") Long functionId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE id IN (SELECT id FROM "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " WHERE function_id = #{functionId} AND version <= #{versionRetentionLine}"
        + " AND deleted_at = 0 LIMIT #{limit})";
  }

  @Override
  public String softDeleteFunctionVersionsByFunctionId(@Param("functionId") Long functionId) {
    return "UPDATE "
        + FunctionVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE function_id = #{functionId} AND deleted_at = 0";
  }
}
