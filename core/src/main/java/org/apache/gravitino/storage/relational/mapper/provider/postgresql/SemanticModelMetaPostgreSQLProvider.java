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

import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.ibatis.annotations.Param;

/** Provides PostgreSQL SQL for Semantic Model create and load operations. */
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
}
