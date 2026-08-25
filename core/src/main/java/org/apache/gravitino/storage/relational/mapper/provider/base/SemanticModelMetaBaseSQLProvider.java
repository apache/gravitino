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

import static org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper.TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper.VERSION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for Semantic Model create and load operations. */
public class SemanticModelMetaBaseSQLProvider {

  private static final String CURRENT_SNAPSHOT_COLUMNS =
      " smm.semantic_model_id, smm.semantic_model_name, smm.metalake_id,"
          + " smm.catalog_id, smm.schema_id, smm.current_version, smm.last_version,"
          + " smm.audit_info, smm.deleted_at, smvi.id,"
          + " smvi.metalake_id as version_metalake_id,"
          + " smvi.catalog_id as version_catalog_id,"
          + " smvi.schema_id as version_schema_id,"
          + " smvi.semantic_model_id as version_semantic_model_id, smvi.version,"
          + " smvi.semantic_model_name as version_semantic_model_name,"
          + " smvi.semantic_model_comment, smvi.semantic_model_definition, smvi.properties,"
          + " smvi.audit_info as version_audit_info,"
          + " smvi.deleted_at as version_deleted_at";

  /** Returns SQL for selecting an active Semantic Model ID by schema ID and name. */
  public String selectSemanticModelIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName) {
    return "SELECT semantic_model_id as semanticModelId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId}"
        + " AND semantic_model_name = #{semanticModelName} AND deleted_at = 0";
  }

  /** Returns SQL for selecting a current Semantic Model snapshot by schema ID and name. */
  public String selectSemanticModelMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName) {
    return "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + " FROM "
        + TABLE_NAME
        + " smm INNER JOIN "
        + VERSION_TABLE_NAME
        + " smvi ON smm.semantic_model_id = smvi.semantic_model_id"
        + " AND smm.current_version = smvi.version"
        + " WHERE smm.schema_id = #{schemaId}"
        + " AND smm.semantic_model_name = #{semanticModelName}"
        + " AND smm.deleted_at = 0 AND smvi.deleted_at = 0";
  }

  /** Returns SQL for selecting a current Semantic Model by fully qualified name. */
  public String selectSemanticModelByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("semanticModelName") String semanticModelName) {
    return """
        SELECT
            mm.metalake_id,
            cm.catalog_id,
            sm.schema_id,
            smm.semantic_model_id,
            smm.semantic_model_name,
            smm.current_version,
            smm.last_version,
            smm.audit_info,
            smm.deleted_at,
            smvi.id,
            smvi.metalake_id as version_metalake_id,
            smvi.catalog_id as version_catalog_id,
            smvi.schema_id as version_schema_id,
            smvi.semantic_model_id as version_semantic_model_id,
            smvi.version,
            smvi.semantic_model_name as version_semantic_model_name,
            smvi.semantic_model_comment,
            smvi.semantic_model_definition,
            smvi.properties,
            smvi.audit_info as version_audit_info,
            smvi.deleted_at as version_deleted_at
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s smm ON sm.schema_id = smm.schema_id
            AND smm.semantic_model_name = #{semanticModelName}
            AND smm.deleted_at = 0
        LEFT JOIN
            %s smvi ON smm.semantic_model_id = smvi.semantic_model_id
            AND smm.current_version = smvi.version
            AND smvi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0
        """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            VERSION_TABLE_NAME);
  }

  /** Returns SQL for inserting a Semantic Model identity row. */
  public String insertSemanticModelMeta(
      @Param("semanticModelMeta") SemanticModelPO semanticModelPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (semantic_model_id, semantic_model_name, metalake_id, catalog_id, schema_id,"
        + " current_version, last_version, audit_info, deleted_at)"
        + " VALUES (#{semanticModelMeta.semanticModelId},"
        + " #{semanticModelMeta.semanticModelName}, #{semanticModelMeta.metalakeId},"
        + " #{semanticModelMeta.catalogId}, #{semanticModelMeta.schemaId},"
        + " #{semanticModelMeta.currentVersion}, #{semanticModelMeta.lastVersion},"
        + " #{semanticModelMeta.auditInfo}, #{semanticModelMeta.deletedAt})";
  }

  /** Returns SQL for inserting or overwriting a Semantic Model identity row. */
  public String insertSemanticModelMetaOnDuplicateKeyUpdate(
      @Param("semanticModelMeta") SemanticModelPO semanticModelPO) {
    return insertSemanticModelMeta(semanticModelPO)
        + " ON DUPLICATE KEY UPDATE"
        + " semantic_model_name = #{semanticModelMeta.semanticModelName},"
        + " metalake_id = #{semanticModelMeta.metalakeId},"
        + " catalog_id = #{semanticModelMeta.catalogId},"
        + " schema_id = #{semanticModelMeta.schemaId},"
        + " current_version = #{semanticModelMeta.currentVersion},"
        + " last_version = #{semanticModelMeta.lastVersion},"
        + " audit_info = #{semanticModelMeta.auditInfo},"
        + " deleted_at = #{semanticModelMeta.deletedAt}";
  }
}
