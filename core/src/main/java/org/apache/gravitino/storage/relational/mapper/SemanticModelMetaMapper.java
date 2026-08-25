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
package org.apache.gravitino.storage.relational.mapper;

import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;

/** A MyBatis mapper for Semantic Model create and load operations. */
public interface SemanticModelMetaMapper {

  /** The Semantic Model identity table name. */
  String TABLE_NAME = "semantic_model_meta";

  /** The Semantic Model version snapshot table name. */
  String VERSION_TABLE_NAME = "semantic_model_version_info";

  /** Declares the nested result mapping for a Semantic Model version snapshot. */
  @Results(
      id = "mapToSemanticModelVersionInfoPO",
      value = {
        @Result(property = "id", column = "id", id = true),
        @Result(property = "metalakeId", column = "version_metalake_id"),
        @Result(property = "catalogId", column = "version_catalog_id"),
        @Result(property = "schemaId", column = "version_schema_id"),
        @Result(property = "semanticModelId", column = "version_semantic_model_id"),
        @Result(property = "version", column = "version"),
        @Result(property = "semanticModelName", column = "version_semantic_model_name"),
        @Result(property = "semanticModelComment", column = "semantic_model_comment"),
        @Result(property = "semanticModelDefinition", column = "semantic_model_definition"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "auditInfo", column = "version_audit_info"),
        @Result(property = "deletedAt", column = "version_deleted_at")
      })
  @Select("SELECT 1")
  SemanticModelVersionInfoPO mapToSemanticModelVersionInfoPO();

  /** Selects an active Semantic Model ID by schema ID and name. */
  @SelectProvider(
      type = SemanticModelMetaSQLProviderFactory.class,
      method = "selectSemanticModelIdBySchemaIdAndName")
  Long selectSemanticModelIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName);

  /** Selects a current Semantic Model snapshot by schema ID and name. */
  @Results(
      id = "semanticModelPOResultMap",
      value = {
        @Result(property = "semanticModelId", column = "semantic_model_id", id = true),
        @Result(property = "semanticModelName", column = "semantic_model_name"),
        @Result(property = "metalakeId", column = "metalake_id"),
        @Result(property = "catalogId", column = "catalog_id"),
        @Result(property = "schemaId", column = "schema_id"),
        @Result(property = "currentVersion", column = "current_version"),
        @Result(property = "lastVersion", column = "last_version"),
        @Result(property = "auditInfo", column = "audit_info"),
        @Result(property = "deletedAt", column = "deleted_at"),
        @Result(
            property = "semanticModelVersionInfoPO",
            javaType = SemanticModelVersionInfoPO.class,
            column =
                "{id,version_metalake_id,version_catalog_id,version_schema_id,"
                    + "version_semantic_model_id,version,version_semantic_model_name,"
                    + "semantic_model_comment,semantic_model_definition,properties,"
                    + "version_audit_info,version_deleted_at}",
            one = @One(resultMap = "mapToSemanticModelVersionInfoPO"))
      })
  @SelectProvider(
      type = SemanticModelMetaSQLProviderFactory.class,
      method = "selectSemanticModelMetaBySchemaIdAndName")
  SemanticModelPO selectSemanticModelMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("semanticModelName") String semanticModelName);

  /** Selects a current Semantic Model snapshot by fully qualified name. */
  @ResultMap("semanticModelPOResultMap")
  @SelectProvider(
      type = SemanticModelMetaSQLProviderFactory.class,
      method = "selectSemanticModelByFullQualifiedName")
  SemanticModelPO selectSemanticModelByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("semanticModelName") String semanticModelName);

  /** Inserts a Semantic Model identity row. */
  @InsertProvider(
      type = SemanticModelMetaSQLProviderFactory.class,
      method = "insertSemanticModelMeta")
  void insertSemanticModelMeta(@Param("semanticModelMeta") SemanticModelPO semanticModelPO);

  /** Inserts or overwrites a Semantic Model identity row. */
  @InsertProvider(
      type = SemanticModelMetaSQLProviderFactory.class,
      method = "insertSemanticModelMetaOnDuplicateKeyUpdate")
  void insertSemanticModelMetaOnDuplicateKeyUpdate(
      @Param("semanticModelMeta") SemanticModelPO semanticModelPO);
}
