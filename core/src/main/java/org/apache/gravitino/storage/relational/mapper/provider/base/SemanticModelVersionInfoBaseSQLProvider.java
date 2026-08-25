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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for Semantic Model version snapshots. */
public class SemanticModelVersionInfoBaseSQLProvider {

  /** Returns SQL for inserting a Semantic Model version snapshot. */
  public String insertSemanticModelVersionInfo(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO) {
    return "INSERT INTO "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " (metalake_id, catalog_id, schema_id, semantic_model_id, version,"
        + " semantic_model_name, semantic_model_comment, semantic_model_definition,"
        + " properties, audit_info, deleted_at)"
        + " VALUES (#{semanticModelVersionInfo.metalakeId},"
        + " #{semanticModelVersionInfo.catalogId}, #{semanticModelVersionInfo.schemaId},"
        + " #{semanticModelVersionInfo.semanticModelId}, #{semanticModelVersionInfo.version},"
        + " #{semanticModelVersionInfo.semanticModelName},"
        + " #{semanticModelVersionInfo.semanticModelComment},"
        + " #{semanticModelVersionInfo.semanticModelDefinition},"
        + " #{semanticModelVersionInfo.properties}, #{semanticModelVersionInfo.auditInfo},"
        + " #{semanticModelVersionInfo.deletedAt})";
  }

  /** Returns SQL for inserting or overwriting a Semantic Model version snapshot. */
  public String insertSemanticModelVersionInfoOnDuplicateKeyUpdate(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO) {
    return insertSemanticModelVersionInfo(versionInfoPO)
        + " ON DUPLICATE KEY UPDATE"
        + " semantic_model_name = #{semanticModelVersionInfo.semanticModelName},"
        + " semantic_model_comment = #{semanticModelVersionInfo.semanticModelComment},"
        + " semantic_model_definition = #{semanticModelVersionInfo.semanticModelDefinition},"
        + " properties = #{semanticModelVersionInfo.properties},"
        + " audit_info = #{semanticModelVersionInfo.auditInfo},"
        + " deleted_at = #{semanticModelVersionInfo.deletedAt}";
  }

  /** Returns SQL for selecting a Semantic Model version snapshot. */
  public String selectSemanticModelVersionInfoBySemanticModelIdAndVersion(
      @Param("semanticModelId") Long semanticModelId, @Param("version") Integer version) {
    return "SELECT id as id, metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId, semantic_model_id as semanticModelId, version as version,"
        + " semantic_model_name as semanticModelName,"
        + " semantic_model_comment as semanticModelComment,"
        + " semantic_model_definition as semanticModelDefinition, properties as properties,"
        + " audit_info as auditInfo, deleted_at as deletedAt FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE semantic_model_id = #{semanticModelId}"
        + " AND version = #{version} AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting all snapshots for a Semantic Model ID. */
  public String softDeleteSemanticModelVersionsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId) {
    return softDeleteBy("semantic_model_id = #{semanticModelId}");
  }

  /** Returns SQL for soft-deleting Semantic Model snapshots by schema IDs. */
  public String softDeleteSemanticModelVersionsBySchemaIds(
      @Param("schemaIds") List<Long> schemaIds) {
    return "<script>UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}</foreach>) AND deleted_at = 0</script>";
  }

  /** Returns SQL for soft-deleting Semantic Model snapshots by catalog ID. */
  public String softDeleteSemanticModelVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy("catalog_id = #{catalogId}");
  }

  /** Returns SQL for soft-deleting Semantic Model snapshots by metalake ID. */
  public String softDeleteSemanticModelVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for permanently deleting old Semantic Model snapshots. */
  public String deleteSemanticModelVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns SQL for finding Semantic Models that exceed a version retention count. */
  public String selectSemanticModelVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return "SELECT semantic_model_id as semanticModelId, MAX(version) as version FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE version > #{versionRetentionCount} AND deleted_at = 0"
        + " GROUP BY semantic_model_id";
  }

  /** Returns SQL for soft-deleting snapshots through a per-model retention line. */
  public String softDeleteSemanticModelVersionsByRetentionLine(
      @Param("semanticModelId") Long semanticModelId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE semantic_model_id = #{semanticModelId}"
        + " AND version <= #{versionRetentionLine} AND deleted_at = 0 LIMIT #{limit}";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
