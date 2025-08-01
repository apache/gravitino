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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.h2.ModelVersionMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.ModelVersionMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ModelVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class ModelVersionMetaSQLProviderFactory {

  static class ModelVersionMetaMySQLProvider extends ModelVersionMetaBaseSQLProvider {}

  private static final Map<JDBCBackendType, ModelVersionMetaBaseSQLProvider>
      MODEL_VERSION_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new ModelVersionMetaMySQLProvider(),
              JDBCBackendType.H2, new ModelVersionMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new ModelVersionMetaPostgreSQLProvider());

  public static ModelVersionMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return MODEL_VERSION_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertModelVersionMetas(
      @Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs) {
    return getProvider().insertModelVersionMetas(modelVersionPOs);
  }

  public static String insertModelVersionMetasWithVersionNumber(
      @Param("modelVersionMetas") List<ModelVersionPO> modelVersionPOs) {
    return getProvider().insertModelVersionMetasWithVersionNumber(modelVersionPOs);
  }

  public static String listModelVersionMetasByModelId(@Param("modelId") Long modelId) {
    return getProvider().listModelVersionMetasByModelId(modelId);
  }

  public static String selectModelVersionMeta(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return getProvider().selectModelVersionMeta(modelId, modelVersion);
  }

  public static String selectModelVersionMetaByAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return getProvider().selectModelVersionMetaByAlias(modelId, alias);
  }

  public static String softDeleteModelVersionsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return getProvider().softDeleteModelVersionsBySchemaIdAndModelName(schemaId, modelName);
  }

  public static String softDeleteModelVersionMetaByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return getProvider().softDeleteModelVersionMetaByModelIdAndVersion(modelId, modelVersion);
  }

  public static String softDeleteModelVersionMetaByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return getProvider().softDeleteModelVersionMetaByModelIdAndAlias(modelId, alias);
  }

  public static String softDeleteModelVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteModelVersionMetasBySchemaId(schemaId);
  }

  public static String softDeleteModelVersionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteModelVersionMetasByCatalogId(catalogId);
  }

  public static String softDeleteModelVersionMetasByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteModelVersionMetasByMetalakeId(metalakeId);
  }

  public static String deleteModelVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteModelVersionMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String updateModelVersionMeta(
      @Param("newModelVersionMeta") ModelVersionPO newModelVersionPO,
      @Param("oldModelVersionMeta") ModelVersionPO oldModelVersionPO) {
    return getProvider().updateModelVersionMeta(newModelVersionPO, oldModelVersionPO);
  }
}
