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
package org.apache.gravitino.storage.relational.mapper.provider.h2;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.ibatis.annotations.Param;

public class ModelVersionMetaH2Provider extends ModelVersionMetaBaseSQLProvider {

  @Override
  public String insertModelVersionMetas(
      @Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs) {
    return "<script>"
        + "INSERT INTO "
        + ModelVersionMetaMapper.TABLE_NAME
        + "(metalake_id, catalog_id, schema_id, model_id, version, model_version_comment,"
        + " model_version_properties, model_version_uri_name, model_version_uri, audit_info, deleted_at)"
        + " SELECT m.metalake_id, m.catalog_id, m.schema_id, m.model_id, m.model_latest_version, v.model_version_comment,"
        + " v.model_version_properties, v.model_version_uri_name, v.model_version_uri, v.audit_info, v.deleted_at"
        + " FROM ("
        + "<foreach collection='modelVersionMetas' item='version' separator='UNION ALL'>"
        + " SELECT"
        + " CAST(#{version.modelId} AS BIGINT) AS model_id, CAST(#{version.modelVersionComment} AS TEXT) AS model_version_comment,"
        + " CAST(#{version.modelVersionProperties} AS MEDIUMTEXT) AS model_version_properties, CAST(#{version.auditInfo} AS MEDIUMTEXT) AS audit_info,"
        + " CAST(#{version.deletedAt} AS BIGINT) AS deleted_at, CAST(#{version.modelVersionUriName} AS VARCHAR(128)) AS model_version_uri_name,"
        + " CAST(#{version.modelVersionUri} AS TEXT) AS model_version_uri "
        + "</foreach>"
        + " ) v"
        + " JOIN"
        + " (SELECT metalake_id, catalog_id, schema_id, model_id, model_latest_version"
        + " FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE model_id = #{modelVersionMetas[0].modelId} AND deleted_at = 0) m"
        + " ON v.model_id = m.model_id"
        + "</script>";
  }

  @Override
  public String insertModelVersionMetasWithVersionNumber(
      @Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs) {
    return "<script>"
        + "INSERT INTO "
        + ModelVersionMetaMapper.TABLE_NAME
        + "(metalake_id, catalog_id, schema_id, model_id, version, model_version_comment,"
        + " model_version_properties, model_version_uri_name, model_version_uri, audit_info, deleted_at)"
        + " SELECT m.metalake_id, m.catalog_id, m.schema_id, m.model_id, v.model_version_number, v.model_version_comment,"
        + " v.model_version_properties, v.model_version_uri_name, v.model_version_uri, v.audit_info, v.deleted_at"
        + " FROM ("
        + "<foreach collection='modelVersionMetas' item='version' separator='UNION ALL'>"
        + " SELECT"
        + " CAST(#{version.modelId} AS BIGINT) AS model_id, CAST(#{version.modelVersionComment} AS TEXT) AS model_version_comment,"
        + " CAST(#{version.modelVersionProperties} AS MEDIUMTEXT) AS model_version_properties, CAST(#{version.auditInfo} AS MEDIUMTEXT) AS audit_info,"
        + " CAST(#{version.deletedAt} AS BIGINT) AS deleted_at, CAST(#{version.modelVersionUriName} AS VARCHAR(128)) AS model_version_uri_name,"
        + " CAST(#{version.modelVersionUri} AS TEXT) AS model_version_uri, CAST(#{version.modelVersion} AS INT) AS model_version_number "
        + "</foreach>"
        + " ) v"
        + " JOIN"
        + " (SELECT metalake_id, catalog_id, schema_id, model_id"
        + " FROM "
        + ModelMetaMapper.TABLE_NAME
        + " WHERE model_id = #{modelVersionMetas[0].modelId} AND deleted_at = 0) m"
        + " ON v.model_id = m.model_id"
        + "</script>";
  }
}
