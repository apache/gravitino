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
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface ModelVersionMetaMapper {

  String TABLE_NAME = "model_version_info";

  @InsertProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "insertModelVersionMetas")
  void insertModelVersionMetas(@Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs);

  @InsertProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "insertModelVersionMetasWithVersionNumber")
  void insertModelVersionMetasWithVersionNumber(
      @Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs);

  @SelectProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "listModelVersionMetasByModelId")
  List<ModelVersionPO> listModelVersionMetasByModelId(@Param("modelId") Long modelId);

  @SelectProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "selectModelVersionMeta")
  List<ModelVersionPO> selectModelVersionMeta(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion);

  @SelectProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "selectModelVersionMetaByAlias")
  List<ModelVersionPO> selectModelVersionMetaByAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionsBySchemaIdAndModelName")
  Integer softDeleteModelVersionsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionMetaByModelIdAndVersion")
  Integer softDeleteModelVersionMetaByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionMetaByModelIdAndAlias")
  Integer softDeleteModelVersionMetaByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionMetasBySchemaId")
  Integer softDeleteModelVersionMetasBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionMetasByCatalogId")
  Integer softDeleteModelVersionMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "softDeleteModelVersionMetasByMetalakeId")
  Integer softDeleteModelVersionMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "deleteModelVersionMetasByLegacyTimeline")
  Integer deleteModelVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(
      type = ModelVersionMetaSQLProviderFactory.class,
      method = "updateModelVersionMeta")
  Integer updateModelVersionMeta(
      @Param("newModelVersionMeta") ModelVersionPO newModelVersionPO,
      @Param("oldModelVersionMeta") ModelVersionPO oldModelVersionPO);
}
