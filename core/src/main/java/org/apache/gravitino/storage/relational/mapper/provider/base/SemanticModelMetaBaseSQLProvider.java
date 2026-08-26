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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for Semantic Model identity metadata. */
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

  /** Returns SQL for listing current Semantic Model snapshots by schema ID. */
  public String listSemanticModelPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + " FROM "
        + TABLE_NAME
        + " smm INNER JOIN "
        + VERSION_TABLE_NAME
        + " smvi ON smm.semantic_model_id = smvi.semantic_model_id"
        + " AND smm.current_version = smvi.version"
        + " WHERE smm.schema_id = #{schemaId}"
        + " AND smm.deleted_at = 0 AND smvi.deleted_at = 0";
  }

  /** Returns SQL for listing current Semantic Model snapshots by qualified schema name. */
  public String listSemanticModelPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return qualifiedNameSelect(false);
  }

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

  /** Returns SQL for optimistically updating a Semantic Model identity row. */
  public String updateSemanticModelMeta(
      @Param("newSemanticModelMeta") SemanticModelPO newSemanticModelPO,
      @Param("oldSemanticModelMeta") SemanticModelPO oldSemanticModelPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET semantic_model_name = #{newSemanticModelMeta.semanticModelName},"
        + " schema_id = #{newSemanticModelMeta.schemaId},"
        + " current_version = #{newSemanticModelMeta.currentVersion},"
        + " last_version = #{newSemanticModelMeta.lastVersion},"
        + " audit_info = #{newSemanticModelMeta.auditInfo},"
        + " deleted_at = #{newSemanticModelMeta.deletedAt}"
        + " WHERE semantic_model_id = #{oldSemanticModelMeta.semanticModelId}"
        + " AND current_version = #{oldSemanticModelMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  /** Returns SQL for listing current Semantic Model snapshots by stable IDs. */
  public String listSemanticModelPOsBySemanticModelIds(
      @Param("semanticModelIds") List<Long> semanticModelIds) {
    return "<script>"
        + "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + " FROM "
        + TABLE_NAME
        + " smm INNER JOIN "
        + VERSION_TABLE_NAME
        + " smvi ON smm.semantic_model_id = smvi.semantic_model_id"
        + " AND smm.current_version = smvi.version"
        + " WHERE smm.semantic_model_id IN "
        + "<foreach item='semanticModelId' collection='semanticModelIds'"
        + " open='(' separator=',' close=')'>#{semanticModelId}</foreach>"
        + " AND smm.deleted_at = 0 AND smvi.deleted_at = 0"
        + "</script>";
  }

  /** Returns SQL for soft-deleting a Semantic Model identity with an optimistic version check. */
  public String softDeleteSemanticModelMetasBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId,
      @Param("currentVersion") Long currentVersion) {
    return softDeleteBy(
        "semantic_model_id = #{semanticModelId} AND current_version = #{currentVersion}");
  }

  /** Returns SQL for soft-deleting Semantic Model identities by metalake ID. */
  public String softDeleteSemanticModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for soft-deleting Semantic Model identities by catalog ID. */
  public String softDeleteSemanticModelMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy("catalog_id = #{catalogId}");
  }

  /** Returns SQL for soft-deleting Semantic Model identities by schema IDs. */
  public String softDeleteSemanticModelMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}</foreach>) AND deleted_at = 0</script>";
  }

  /** Returns SQL for permanently deleting old Semantic Model identity rows. */
  public String deleteSemanticModelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns SQL for selecting a current Semantic Model by fully qualified name. */
  public String selectSemanticModelByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("semanticModelName") String semanticModelName) {
    return qualifiedNameSelect(true);
  }

  private String qualifiedNameSelect(boolean filterBySemanticModelName) {
    String semanticModelNameCondition =
        filterBySemanticModelName ? " AND smm.semantic_model_name = #{semanticModelName}" : "";
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
            %s smm ON sm.schema_id = smm.schema_id%s
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
            semanticModelNameCondition,
            VERSION_TABLE_NAME);
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
