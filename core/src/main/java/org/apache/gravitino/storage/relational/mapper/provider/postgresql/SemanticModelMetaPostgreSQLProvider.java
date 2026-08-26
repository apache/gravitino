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

import static org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.ibatis.annotations.Param;

/** Provides PostgreSQL SQL for Semantic Model identity metadata. */
public class SemanticModelMetaPostgreSQLProvider extends SemanticModelMetaBaseSQLProvider {

  @Override
  public String insertSemanticModelMetaOnDuplicateKeyUpdate(
      @Param("semanticModelMeta") SemanticModelPO semanticModelPO) {
    return insertSemanticModelMeta(semanticModelPO)
        + " ON CONFLICT (semantic_model_id) DO UPDATE SET"
        + " semantic_model_name = #{semanticModelMeta.semanticModelName},"
        + " metalake_id = #{semanticModelMeta.metalakeId},"
        + " catalog_id = #{semanticModelMeta.catalogId},"
        + " schema_id = #{semanticModelMeta.schemaId},"
        + " current_version = #{semanticModelMeta.currentVersion},"
        + " last_version = #{semanticModelMeta.lastVersion},"
        + " audit_info = #{semanticModelMeta.auditInfo},"
        + " deleted_at = #{semanticModelMeta.deletedAt}";
  }

  @Override
  public String softDeleteSemanticModelMetasBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId,
      @Param("currentVersion") Long currentVersion) {
    return softDeleteBy(
        "semantic_model_id = #{semanticModelId} AND current_version = #{currentVersion}");
  }

  @Override
  public String softDeleteSemanticModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  @Override
  public String softDeleteSemanticModelMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy("catalog_id = #{catalogId}");
  }

  @Override
  public String softDeleteSemanticModelMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}</foreach>) AND deleted_at = 0</script>";
  }

  @Override
  public String deleteSemanticModelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE semantic_model_id IN (SELECT semantic_model_id FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
