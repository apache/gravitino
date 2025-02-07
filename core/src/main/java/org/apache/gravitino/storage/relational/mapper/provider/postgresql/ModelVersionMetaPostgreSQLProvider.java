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
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ModelVersionMetaPostgreSQLProvider extends ModelVersionMetaBaseSQLProvider {

  @Override
  public String softDeleteModelVersionsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " mvi SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE mvi.schema_id = #{schemaId} AND mvi.model_id = ("
        + " SELECT mm.model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mm WHERE mm.schema_id = #{schemaId} AND mm.model_name = #{modelName}"
        + " AND mm.deleted_at = 0) AND mvi.deleted_at = 0";
  }

  @Override
  public String softDeleteModelVersionMetaByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE model_id = #{modelId} AND version = #{modelVersion} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelVersionMetaByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE model_id = #{modelId} AND version = ("
        + " SELECT model_version FROM "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND model_version_alias = #{alias} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelVersionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteModelVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String deleteModelVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ModelVersionMetaMapper.TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
