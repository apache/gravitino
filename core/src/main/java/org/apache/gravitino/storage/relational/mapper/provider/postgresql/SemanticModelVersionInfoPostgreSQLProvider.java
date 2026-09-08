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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelVersionInfoBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.Param;

/** Provides PostgreSQL SQL for Semantic Model version snapshots. */
public class SemanticModelVersionInfoPostgreSQLProvider
    extends SemanticModelVersionInfoBaseSQLProvider {

  @Override
  public String insertSemanticModelVersionInfoOnDuplicateKeyUpdate(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO) {
    return insertSemanticModelVersionInfo(versionInfoPO)
        + " ON CONFLICT (semantic_model_id, version, deleted_at) DO UPDATE SET"
        + " semantic_model_name = #{semanticModelVersionInfo.semanticModelName},"
        + " semantic_model_comment = #{semanticModelVersionInfo.semanticModelComment},"
        + " semantic_model_definition = #{semanticModelVersionInfo.semanticModelDefinition},"
        + " properties = #{semanticModelVersionInfo.properties},"
        + " audit_info = #{semanticModelVersionInfo.auditInfo},"
        + " deleted_at = #{semanticModelVersionInfo.deletedAt}";
  }

  @Override
  public String softDeleteSemanticModelVersionsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId) {
    return softDeleteBy("semantic_model_id = #{semanticModelId}");
  }

  @Override
  public String softDeleteSemanticModelVersionsBySchemaIds(
      @Param("schemaIds") List<Long> schemaIds) {
    return "<script>UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}</foreach>) AND deleted_at = 0</script>";
  }

  @Override
  public String softDeleteSemanticModelVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy("catalog_id = #{catalogId}");
  }

  @Override
  public String softDeleteSemanticModelVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  @Override
  public String deleteSemanticModelVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String softDeleteSemanticModelVersionsByRetentionLine(
      @Param("semanticModelId") Long semanticModelId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE id IN (SELECT id FROM "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " WHERE semantic_model_id = #{semanticModelId}"
        + " AND version <= #{versionRetentionLine}"
        + " AND deleted_at = 0 LIMIT #{limit})";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + SemanticModelVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
