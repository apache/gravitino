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
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface ModelMetaMapper {
  String TABLE_NAME = "model_meta";

  @InsertProvider(type = ModelMetaSQLProviderFactory.class, method = "insertModelMeta")
  void insertModelMeta(@Param("modelMeta") ModelPO modelPO);

  @InsertProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "insertModelMetaOnDuplicateKeyUpdate")
  void insertModelMetaOnDuplicateKeyUpdate(@Param("modelMeta") ModelPO modelPO);

  @SelectProvider(type = ModelMetaSQLProviderFactory.class, method = "listModelPOsBySchemaId")
  List<ModelPO> listModelPOsBySchemaId(@Param("schemaId") Long schemaId);

  @SelectProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "selectModelMetaBySchemaIdAndModelName")
  ModelPO selectModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName);

  @SelectProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "selectModelIdBySchemaIdAndModelName")
  Long selectModelIdBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName);

  @SelectProvider(type = ModelMetaSQLProviderFactory.class, method = "selectModelMetaByModelId")
  ModelPO selectModelMetaByModelId(@Param("modelId") Long modelId);

  @UpdateProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "softDeleteModelMetaBySchemaIdAndModelName")
  Integer softDeleteModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName);

  @UpdateProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "softDeleteModelMetasByCatalogId")
  Integer softDeleteModelMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "softDeleteModelMetasByMetalakeId")
  Integer softDeleteModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "softDeleteModelMetasBySchemaId")
  Integer softDeleteModelMetasBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = ModelMetaSQLProviderFactory.class,
      method = "deleteModelMetasByLegacyTimeline")
  Integer deleteModelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(type = ModelMetaSQLProviderFactory.class, method = "updateModelLatestVersion")
  Integer updateModelLatestVersion(@Param("modelId") Long modelId);
}
