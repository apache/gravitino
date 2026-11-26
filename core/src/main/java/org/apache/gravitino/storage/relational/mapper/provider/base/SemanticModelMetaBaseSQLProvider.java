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
import javax.annotation.Nullable;
import org.apache.gravitino.semantic.SemanticModelRevisionSelector;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for Semantic Model identity metadata. */
public class SemanticModelMetaBaseSQLProvider {

  private static final String MODEL_COLUMNS =
      " smm.semantic_model_id, smm.semantic_model_name, smm.current_version,"
          + " smm.active_version, smm.last_version, smm.audit_info, smm.deleted_at";

  private static final String SNAPSHOT_COLUMNS =
      ", smvi.id, smvi.metalake_id AS version_metalake_id,"
          + " smvi.catalog_id AS version_catalog_id, smvi.schema_id AS version_schema_id,"
          + " smvi.semantic_model_id AS version_semantic_model_id, smvi.version,"
          + " smvi.publication_status, smvi.published_by, smvi.published_at,"
          + " smvi.semantic_model_comment, smvi.datasets, smvi.relationships, smvi.metrics,"
          + " smvi.ai_context, smvi.custom_extensions, smvi.properties,"
          + " smvi.audit_info AS version_audit_info, smvi.deleted_at AS version_deleted_at";

  /** Returns SQL for listing Semantic Models with the selected snapshots by schema ID. */
  public String listSemanticModelPOsBySchemaId(
      @Param("schemaId") Long schemaId,
      @Param("selector") @Nullable SemanticModelRevisionSelector selector) {
    return selectModels(selector) + " AND smm.schema_id = #{schemaId}";
  }

  /** Returns SQL for listing Semantic Models by qualified schema name. */
  public String listSemanticModelPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("selector") @Nullable SemanticModelRevisionSelector selector) {
    return qualifiedNameSelect(false, selector);
  }

  /**
   * Joins the selected live snapshot to a model identity. A null selector reads identity only.
   *
   * @param model The model alias declared by the provider.
   * @param snapshot The snapshot alias declared by the provider.
   * @param selector The snapshot selector, or null for identity-only reads.
   * @return The join clause.
   */
  public static String snapshotJoin(
      String model, String snapshot, @Nullable SemanticModelRevisionSelector selector) {
    if (selector == null) {
      return "";
    }
    String version;
    if (selector == SemanticModelRevisionSelector.ACTIVE) {
      version = model + ".active_version";
    } else if (selector == SemanticModelRevisionSelector.LATEST) {
      version = model + ".current_version";
    } else {
      version = "#{selector.number}";
    }
    return " LEFT JOIN "
        + VERSION_TABLE_NAME
        + " "
        + snapshot
        + " ON "
        + snapshot
        + ".semantic_model_id = "
        + model
        + ".semantic_model_id"
        + " AND "
        + snapshot
        + ".version = "
        + version
        + " AND "
        + snapshot
        + ".deleted_at = 0";
  }

  /** Returns SQL for selecting an active Semantic Model ID by schema ID and name. */
  public String selectSemanticModelIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName) {
    return "SELECT semantic_model_id as semanticModelId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId}"
        + " AND semantic_model_name = #{semanticModelName} AND deleted_at = 0";
  }

  /** Returns SQL for selecting a Semantic Model by schema ID and name. */
  public String selectSemanticModelMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId,
      @Param("semanticModelName") String semanticModelName,
      @Param("selector") @Nullable SemanticModelRevisionSelector selector) {
    return selectModels(selector)
        + " AND smm.schema_id = #{schemaId}"
        + " AND smm.semantic_model_name = #{semanticModelName}";
  }

  /** Returns SQL for selecting and locking an active Semantic Model identity by natural key. */
  public String selectSemanticModelMetaBySchemaIdAndNameForUpdate(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName) {
    return "SELECT semantic_model_id as semanticModelId,"
        + " semantic_model_name as semanticModelName, metalake_id as metalakeId,"
        + " catalog_id as catalogId, schema_id as schemaId,"
        + " current_version as currentVersion, active_version as activeVersion,"
        + " last_version as lastVersion,"
        + " audit_info as auditInfo, deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId}"
        + " AND semantic_model_name = #{semanticModelName}"
        + " AND deleted_at = 0 FOR UPDATE";
  }

  /** Returns SQL for selecting a Semantic Model's requested snapshot by stable ID. */
  public String selectSemanticModelMetaById(
      @Param("semanticModelId") Long semanticModelId,
      @Param("selector") SemanticModelRevisionSelector selector) {
    return selectModels(selector) + " AND smm.semantic_model_id = #{semanticModelId}";
  }

  /** Returns SQL for selecting and locking a Semantic Model identity by stable ID. */
  public String selectSemanticModelMetaByIdForUpdate(
      @Param("semanticModelId") Long semanticModelId) {
    return "SELECT semantic_model_id as semanticModelId,"
        + " semantic_model_name as semanticModelName, metalake_id as metalakeId,"
        + " catalog_id as catalogId, schema_id as schemaId,"
        + " current_version as currentVersion, active_version as activeVersion,"
        + " last_version as lastVersion,"
        + " audit_info as auditInfo, deleted_at as deletedAt FROM "
        + TABLE_NAME
        + " WHERE semantic_model_id = #{semanticModelId} AND deleted_at = 0 FOR UPDATE";
  }

  /** Returns SQL for inserting a Semantic Model identity row. */
  public String insertSemanticModelMeta(
      @Param("semanticModelMeta") SemanticModelPO semanticModelPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (semantic_model_id, semantic_model_name, metalake_id, catalog_id, schema_id,"
        + " current_version, active_version, last_version, audit_info, deleted_at)"
        + " VALUES (#{semanticModelMeta.semanticModelId},"
        + " #{semanticModelMeta.semanticModelName}, #{semanticModelMeta.metalakeId},"
        + " #{semanticModelMeta.catalogId}, #{semanticModelMeta.schemaId},"
        + " #{semanticModelMeta.currentVersion}, #{semanticModelMeta.activeVersion},"
        + " #{semanticModelMeta.lastVersion},"
        + " #{semanticModelMeta.auditInfo}, #{semanticModelMeta.deletedAt})";
  }

  /** Returns SQL for updating a Semantic Model identity with a version check. */
  public String updateSemanticModelMeta(
      @Param("newSemanticModelMeta") SemanticModelPO newSemanticModelPO,
      @Param("oldSemanticModelMeta") SemanticModelPO oldSemanticModelPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET semantic_model_name = #{newSemanticModelMeta.semanticModelName},"
        + " audit_info = #{newSemanticModelMeta.auditInfo},"
        + " current_version = #{newSemanticModelMeta.currentVersion},"
        + " last_version = #{newSemanticModelMeta.lastVersion}"
        + (newSemanticModelPO.getActiveVersion() == 0
            ? ""
            : ", active_version = #{newSemanticModelMeta.activeVersion}")
        + " WHERE semantic_model_id = #{oldSemanticModelMeta.semanticModelId}"
        + " AND semantic_model_name = #{oldSemanticModelMeta.semanticModelName}"
        + " AND current_version = #{oldSemanticModelMeta.currentVersion}"
        + " AND deleted_at = 0"
        + " AND NOT EXISTS (SELECT 1 FROM "
        + VERSION_TABLE_NAME
        + " smvi WHERE smvi.semantic_model_id = #{oldSemanticModelMeta.semanticModelId}"
        + " AND smvi.version >= #{newSemanticModelMeta.currentVersion}"
        + " AND smvi.deleted_at = 0)";
  }

  /** Returns SQL for soft-deleting a Semantic Model identity guarded by the observed name. */
  public String softDeleteSemanticModelMetasBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId,
      @Param("semanticModelName") String semanticModelName) {
    return softDeleteBy(
        "semantic_model_id = #{semanticModelId} AND semantic_model_name = #{semanticModelName}");
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

  /** Returns SQL for selecting a Semantic Model by fully qualified name. */
  public String selectSemanticModelByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("semanticModelName") String semanticModelName,
      @Param("selector") @Nullable SemanticModelRevisionSelector selector) {
    return qualifiedNameSelect(true, selector);
  }

  private static String selectModels(@Nullable SemanticModelRevisionSelector selector) {
    return "SELECT smm.metalake_id, smm.catalog_id, smm.schema_id,"
        + MODEL_COLUMNS
        + (selector == null ? "" : SNAPSHOT_COLUMNS)
        + " FROM "
        + TABLE_NAME
        + " smm"
        + snapshotJoin("smm", "smvi", selector)
        + " WHERE smm.deleted_at = 0";
  }

  private String qualifiedNameSelect(
      boolean filterBySemanticModelName, @Nullable SemanticModelRevisionSelector selector) {
    String semanticModelNameCondition =
        filterBySemanticModelName ? " AND smm.semantic_model_name = #{semanticModelName}" : "";
    return """
        SELECT mm.metalake_id, cm.catalog_id, sm.schema_id, %s
        FROM %s mm
        INNER JOIN %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName} AND cm.deleted_at = 0
        LEFT JOIN %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName} AND sm.deleted_at = 0
        LEFT JOIN %s smm ON sm.schema_id = smm.schema_id%s AND smm.deleted_at = 0
        %s
        WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0
        """
        .formatted(
            MODEL_COLUMNS + (selector == null ? "" : SNAPSHOT_COLUMNS),
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            semanticModelNameCondition,
            snapshotJoin("smm", "smvi", selector));
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
