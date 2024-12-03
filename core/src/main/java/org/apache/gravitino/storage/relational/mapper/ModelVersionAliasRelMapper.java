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
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface ModelVersionAliasRelMapper {

  String TABLE_NAME = "model_version_alias_rel";

  @InsertProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "insertModelVersionAliasRel")
  void insertModelVersionAliasRel(
      @Param("modelVersionAliasRel") List<ModelVersionAliasRelPO> modelVersionAliasRelPOs);

  @SelectProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "selectModelVersionAliasRelByModelId")
  List<ModelVersionAliasRelPO> selectModelVersionAliasRelByModelId(@Param("modelId") Long modelId);

  @SelectProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "selectModelVersionAliasRelByModelIdAndVersion")
  List<ModelVersionAliasRelPO> selectModelVersionAliasRelByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion);

  @SelectProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "selectModelVersionAliasRelByModelIdAndAlias")
  List<ModelVersionAliasRelPO> selectModelVersionAliasRelByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelBySchemaIdAndModelName")
  Integer softDeleteModelVersionAliasRelBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelByModelIdAndVersion")
  Integer softDeleteModelVersionAliasRelByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelByModelIdAndAlias")
  Integer softDeleteModelVersionAliasRelByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelsBySchemaId")
  Integer softDeleteModelVersionAliasRelsBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelsByCatalogId")
  Integer softDeleteModelVersionAliasRelsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "softDeleteModelVersionAliasRelsByMetalakeId")
  Integer softDeleteModelVersionAliasRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = ModelVersionAliasSQLProviderFactory.class,
      method = "deleteModelVersionAliasRelsByLegacyTimeline")
  Integer deleteModelVersionAliasRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
