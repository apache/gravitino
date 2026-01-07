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

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.ibatis.annotations.Param;

public class ModelMetaPostgreSQLProvider extends ModelMetaBaseSQLProvider {

  @Override
  public String insertModelMetaOnDuplicateKeyUpdate(@Param("modelMeta") ModelPO modelPO) {
    return "INSERT INTO "
        + ModelMetaMapper.TABLE_NAME
        + " (model_id, model_name, metalake_id, catalog_id, schema_id,"
        + " model_comment, model_properties, model_latest_version, audit_info, deleted_at)"
        + " VALUES (#{modelMeta.modelId}, #{modelMeta.modelName}, #{modelMeta.metalakeId},"
        + " #{modelMeta.catalogId}, #{modelMeta.schemaId}, #{modelMeta.modelComment},"
        + " #{modelMeta.modelProperties}, #{modelMeta.modelLatestVersion}, #{modelMeta.auditInfo},"
        + " #{modelMeta.deletedAt})"
        + " ON CONFLICT (model_id) DO UPDATE SET"
        + " model_name = #{modelMeta.modelName},"
        + " metalake_id = #{modelMeta.metalakeId},"
        + " catalog_id = #{modelMeta.catalogId},"
        + " schema_id = #{modelMeta.schemaId},"
        + " model_comment = #{modelMeta.modelComment},"
        + " model_properties = #{modelMeta.modelProperties},"
        + " model_latest_version = #{modelMeta.modelLatestVersion},"
        + " audit_info = #{modelMeta.auditInfo},"
        + " deleted_at = #{modelMeta.deletedAt}";
  }

  @Override
  public String softDeleteModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id = #{schemaId} AND model_name = #{modelName} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String deleteModelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE model_id IN (SELECT model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String updateModelMeta(
      @Param("newModelMeta") ModelPO newModelPO, @Param("oldModelMeta") ModelPO oldModelPO) {
    return "UPDATE "
        + ModelMetaMapper.TABLE_NAME
        + " SET model_name = #{newModelMeta.modelName},"
        + " metalake_id = #{newModelMeta.metalakeId},"
        + " catalog_id = #{newModelMeta.catalogId},"
        + " schema_id = #{newModelMeta.schemaId},"
        + " model_comment = #{newModelMeta.modelComment},"
        + " model_properties = #{newModelMeta.modelProperties},"
        + " model_latest_version = #{newModelMeta.modelLatestVersion},"
        + " audit_info = #{newModelMeta.auditInfo},"
        + " deleted_at = #{newModelMeta.deletedAt}"
        + " WHERE model_id = #{oldModelMeta.modelId}"
        + " AND model_name = #{oldModelMeta.modelName}"
        + " AND metalake_id = #{oldModelMeta.metalakeId}"
        + " AND catalog_id = #{oldModelMeta.catalogId}"
        + " AND schema_id = #{oldModelMeta.schemaId}"
        + " AND (model_comment = #{oldModelMeta.modelComment}"
        + "   OR (CAST(model_comment AS VARCHAR) IS NULL"
        + "   AND CAST(#{oldModelMeta.modelComment} AS VARCHAR) IS NULL))"
        + " AND model_properties = #{oldModelMeta.modelProperties}"
        + " AND model_latest_version = #{oldModelMeta.modelLatestVersion}"
        + " AND audit_info = #{oldModelMeta.auditInfo}"
        + " AND deleted_at = 0";
  }
}
