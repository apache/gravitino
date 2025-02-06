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
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.ModelMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class ModelMetaSQLProviderFactory {

  static class ModelMetaMySQLProvider extends ModelMetaBaseSQLProvider {}

  static class ModelMetaH2Provider extends ModelMetaBaseSQLProvider {}

  private static final Map<JDBCBackendType, ModelMetaBaseSQLProvider> MODEL_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new ModelMetaMySQLProvider(),
          JDBCBackendType.H2, new ModelMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new ModelMetaPostgreSQLProvider());

  public static ModelMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return MODEL_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertModelMeta(@Param("modelMeta") ModelPO modelPO) {
    return getProvider().insertModelMeta(modelPO);
  }

  public static String insertModelMetaOnDuplicateKeyUpdate(@Param("modelMeta") ModelPO modelPO) {
    return getProvider().insertModelMetaOnDuplicateKeyUpdate(modelPO);
  }

  public static String listModelPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listModelPOsBySchemaId(schemaId);
  }

  public static String selectModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return getProvider().selectModelMetaBySchemaIdAndModelName(schemaId, modelName);
  }

  public static String selectModelIdBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return getProvider().selectModelIdBySchemaIdAndModelName(schemaId, modelName);
  }

  public static String selectModelMetaByModelId(@Param("modelId") Long modelId) {
    return getProvider().selectModelMetaByModelId(modelId);
  }

  public static String softDeleteModelMetaBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return getProvider().softDeleteModelMetaBySchemaIdAndModelName(schemaId, modelName);
  }

  public static String softDeleteModelMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteModelMetasByCatalogId(catalogId);
  }

  public static String softDeleteModelMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteModelMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteModelMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteModelMetasBySchemaId(schemaId);
  }

  public static String deleteModelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteModelMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String updateModelLatestVersion(@Param("modelId") Long modelId) {
    return getProvider().updateModelLatestVersion(modelId);
  }
}
