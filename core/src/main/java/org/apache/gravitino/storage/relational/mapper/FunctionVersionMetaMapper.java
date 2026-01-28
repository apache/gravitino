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
import org.apache.gravitino.storage.relational.po.FunctionMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FunctionVersionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface FunctionVersionMetaMapper {

  String TABLE_NAME = "function_version_info";

  @InsertProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "insertFunctionVersionMeta")
  void insertFunctionVersionMeta(@Param("functionVersionMeta") FunctionVersionPO functionVersionPO);

  @InsertProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "insertFunctionVersionMetaOnDuplicateKeyUpdate")
  void insertFunctionVersionMetaOnDuplicateKeyUpdate(
      @Param("functionVersionMeta") FunctionVersionPO functionVersionPO);

  @UpdateProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionVersionMetasBySchemaId")
  Integer softDeleteFunctionVersionMetasBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionVersionMetasByCatalogId")
  Integer softDeleteFunctionVersionMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionVersionMetasByMetalakeId")
  Integer softDeleteFunctionVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "deleteFunctionVersionMetasByLegacyTimeline")
  Integer deleteFunctionVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "selectFunctionVersionsByRetentionCount")
  List<FunctionMaxVersionPO> selectFunctionVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  @UpdateProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionVersionsByRetentionLine")
  Integer softDeleteFunctionVersionsByRetentionLine(
      @Param("functionId") Long functionId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);

  @UpdateProvider(
      type = FunctionVersionMetaSQLProviderFactory.class,
      method = "softDeleteFunctionVersionsByFunctionId")
  Integer softDeleteFunctionVersionsByFunctionId(@Param("functionId") Long functionId);
}
