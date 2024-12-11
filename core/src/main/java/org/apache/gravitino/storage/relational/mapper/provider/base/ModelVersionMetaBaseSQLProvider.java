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

import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.ibatis.annotations.Param;

public class ModelVersionMetaBaseSQLProvider {

  public String insertModelVersionMeta(@Param("modelVersionMeta") ModelVersionPO modelVersionPO) {
    return "INSERT INTO "
        + ModelVersionMetaMapper.TABLE_NAME
        + "(metalake_id, catalog_id, schema_id, model_id, version,"
        + " model_version_comment, model_version_properties, model_version_uri,"
        + " audit_info, deleted_at)"
        + " SELECT metalake_id, catalog_id, schema_id, model_id, model_latest_version,"
        + " #{modelVersionMeta.modelVersionComment}, #{modelVersionMeta.modelVersionProperties},"
        + " #{modelVersionMeta.modelVersionUri}, #{modelVersionMeta.auditInfo},"
        + " #{modelVersionMeta.deletedAt}"
        + " FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE model_id = #{modelVersionMeta.modelId} AND deleted_at = 0";
  }

  public String listModelVersionMetasByModelId(@Param("modelId") Long modelId) {
    return "SELECT metalake_id AS metalakeId, catalog_id AS catalogId,"
        + " schema_id AS schemaId, model_id AS modelId, version AS modelVersion,"
        + " model_version_comment AS modelVersionComment, model_version_properties AS"
        + " modelVersionProperties, model_version_uri AS modelVersionUri, audit_info AS"
        + " auditInfo, deleted_at AS deletedAt"
        + " FROM "
        + ModelVersionMetaMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND deleted_at = 0";
  }

  public String selectModelVersionMeta(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return "SELECT metalake_id AS metalakeId, catalog_id AS catalogId,"
        + " schema_id AS schemaId, model_id AS modelId, version AS modelVersion,"
        + " model_version_comment AS modelVersionComment, model_version_properties AS"
        + " modelVersionProperties, model_version_uri AS modelVersionUri, audit_info AS"
        + " auditInfo, deleted_at AS deletedAt"
        + " FROM "
        + ModelVersionMetaMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND version = #{modelVersion} AND deleted_at = 0";
  }

  public String selectModelVersionMetaByAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "SELECT mvi.metalake_id AS metalakeId, mvi.catalog_id AS catalogId,"
        + " mvi.schema_id AS schemaId, mvi.model_id AS modelId, mvi.version AS modelVersion,"
        + " mvi.model_version_comment AS modelVersionComment, mvi.model_version_properties AS"
        + " modelVersionProperties, mvi.model_version_uri AS modelVersionUri, mvi.audit_info AS"
        + " auditInfo, mvi.deleted_at AS deletedAt"
        + " FROM "
        + ModelVersionMetaMapper.TABLE_NAME
        + " mvi"
        + " JOIN "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " mvar"
        + " ON mvi.model_id = mvar.model_id AND mvi.version = mvar.model_version"
        + " WHERE mvi.model_id = #{modelId} AND mvar.model_version_alias = #{alias}"
        + " AND mvi.deleted_at = 0 AND mvar.deleted_at = 0";
  }

  public String softDeleteModelVersionsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " mvi SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE mvi.schema_id = #{schemaId} AND mvi.model_id = ("
        + " SELECT mm.model_id FROM "
        + ModelMetaMapper.TABLE_NAME
        + " mm WHERE mm.schema_id = #{schemaId} AND mm.model_name = #{modelName}"
        + " AND mm.deleted_at = 0) AND mvi.deleted_at = 0";
  }

  public String softDeleteModelVersionMetaByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE model_id = #{modelId} AND version = #{modelVersion} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetaByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE model_id = #{modelId} AND version = ("
        + " SELECT model_version FROM "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND model_version_alias = #{alias} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteModelVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ModelVersionMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String deleteModelVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ModelVersionMetaMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
