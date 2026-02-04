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

import java.util.List;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.gravitino.storage.relational.po.FunctionVersionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis Mapper for function metadata operation SQLs. */
public interface FunctionMetaMapper {
  String TABLE_NAME = "function_meta";
  String VERSION_TABLE_NAME = "function_version_info";

  @Results(
      id = "mapToFunctionVersionPO",
      value = {
        @Result(property = "id", column = "id", id = true),
        @Result(property = "metalakeId", column = "version_metalake_id"),
        @Result(property = "catalogId", column = "version_catalog_id"),
        @Result(property = "schemaId", column = "version_schema_id"),
        @Result(property = "functionId", column = "version_function_id"),
        @Result(property = "functionVersion", column = "version"),
        @Result(property = "functionComment", column = "function_comment"),
        @Result(property = "definitions", column = "definitions"),
        @Result(property = "auditInfo", column = "version_audit_info"),
        @Result(property = "deletedAt", column = "version_deleted_at")
      })
  @Select("SELECT 1") // Dummy SQL to avoid MyBatis error, never be executed
  FunctionVersionPO mapToFunctionVersionPO();

  @InsertProvider(type = FunctionMetaSQLProviderFactory.class, method = "insertFunctionMeta")
  void insertFunctionMeta(@Param("functionMeta") FunctionPO functionPO);

  @InsertProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "insertFunctionMetaOnDuplicateKeyUpdate")
  void insertFunctionMetaOnDuplicateKeyUpdate(@Param("functionMeta") FunctionPO functionPO);

  @Results({
    @Result(property = "functionId", column = "function_id", id = true),
    @Result(property = "functionName", column = "function_name"),
    @Result(property = "metalakeId", column = "metalake_id"),
    @Result(property = "catalogId", column = "catalog_id"),
    @Result(property = "schemaId", column = "schema_id"),
    @Result(property = "functionType", column = "function_type"),
    @Result(property = "deterministic", column = "deterministic"),
    @Result(property = "functionCurrentVersion", column = "function_current_version"),
    @Result(property = "functionLatestVersion", column = "function_latest_version"),
    @Result(property = "auditInfo", column = "audit_info"),
    @Result(property = "deletedAt", column = "deleted_at"),
    @Result(
        property = "functionVersionPO",
        javaType = FunctionVersionPO.class,
        column =
            "{id,version_metalake_id,version_catalog_id,version_schema_id,version_function_id,"
                + "version,function_comment,definitions,version_audit_info,version_deleted_at}",
        one = @One(resultMap = "mapToFunctionVersionPO"))
  })
  @SelectProvider(type = FunctionMetaSQLProviderFactory.class, method = "listFunctionPOsBySchemaId")
  List<FunctionPO> listFunctionPOsBySchemaId(@Param("schemaId") Long schemaId);

  @Results(
      id = "functionPOResultMap",
      value = {
        @Result(property = "functionId", column = "function_id", id = true),
        @Result(property = "functionName", column = "function_name"),
        @Result(property = "metalakeId", column = "metalake_id"),
        @Result(property = "catalogId", column = "catalog_id"),
        @Result(property = "schemaId", column = "schema_id"),
        @Result(property = "functionType", column = "function_type"),
        @Result(property = "deterministic", column = "deterministic"),
        @Result(property = "functionCurrentVersion", column = "function_current_version"),
        @Result(property = "functionLatestVersion", column = "function_latest_version"),
        @Result(property = "auditInfo", column = "audit_info"),
        @Result(property = "deletedAt", column = "deleted_at"),
        @Result(
            property = "functionVersionPO",
            javaType = FunctionVersionPO.class,
            column =
                "{id,version_metalake_id,version_catalog_id,version_schema_id,version_function_id,"
                    + "version,function_comment,definitions,version_audit_info,version_deleted_at}",
            one = @One(resultMap = "mapToFunctionVersionPO"))
      })
  @SelectProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "listFunctionPOsByFullQualifiedName")
  List<FunctionPO> listFunctionPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName);

  @ResultMap("functionPOResultMap")
  @SelectProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "selectFunctionMetaByFullQualifiedName")
  FunctionPO selectFunctionMetaByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("functionName") String functionName);

  @ResultMap("functionPOResultMap")
  @SelectProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "selectFunctionMetaBySchemaIdAndName")
  FunctionPO selectFunctionMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("functionName") String functionName);

  @UpdateProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionMetaByFunctionId")
  Integer softDeleteFunctionMetaByFunctionId(@Param("functionId") Long functionId);

  @UpdateProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionMetasByCatalogId")
  Integer softDeleteFunctionMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionMetasByMetalakeId")
  Integer softDeleteFunctionMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionMetasBySchemaId")
  Integer softDeleteFunctionMetasBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = FunctionMetaSQLProviderFactory.class,
      method = "deleteFunctionMetasByLegacyTimeline")
  Integer deleteFunctionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(type = FunctionMetaSQLProviderFactory.class, method = "updateFunctionMeta")
  Integer updateFunctionMeta(
      @Param("newFunctionMeta") FunctionPO newFunctionPO,
      @Param("oldFunctionMeta") FunctionPO oldFunctionPO);
}
