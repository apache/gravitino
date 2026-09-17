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

import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for creating Semantic Model version snapshots. */
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
}
