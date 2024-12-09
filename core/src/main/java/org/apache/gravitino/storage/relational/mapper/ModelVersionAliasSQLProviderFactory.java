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
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionAliasRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.ModelVersionAliasRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ModelVersionAliasRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class ModelVersionAliasSQLProviderFactory {

  static class ModelVersionAliasRelMySQLProvider extends ModelVersionAliasRelBaseSQLProvider {}

  static class ModelVersionAliasRelH2Provider extends ModelVersionAliasRelBaseSQLProvider {}

  private static final Map<JDBCBackendType, ModelVersionAliasRelBaseSQLProvider>
      MODEL_VERSION_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new ModelVersionAliasRelMySQLProvider(),
              JDBCBackendType.H2, new ModelVersionAliasRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new ModelVersionAliasRelPostgreSQLProvider());

  public static ModelVersionAliasRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return MODEL_VERSION_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertModelVersionAliasRels(
      @Param("modelVersionAliasRel") List<ModelVersionAliasRelPO> modelVersionAliasRelPOs) {
    return getProvider().insertModelVersionAliasRels(modelVersionAliasRelPOs);
  }

  public static String selectModelVersionAliasRelsByModelId(@Param("modelId") Long modelId) {
    return getProvider().selectModelVersionAliasRelsByModelId(modelId);
  }

  public static String selectModelVersionAliasRelsByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return getProvider().selectModelVersionAliasRelsByModelIdAndVersion(modelId, modelVersion);
  }

  public static String selectModelVersionAliasRelsByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return getProvider().selectModelVersionAliasRelsByModelIdAndAlias(modelId, alias);
  }

  public static String softDeleteModelVersionAliasRelsBySchemaIdAndModelName(
      @Param("schemaId") Long schemaId, @Param("modelName") String modelName) {
    return getProvider().softDeleteModelVersionAliasRelsBySchemaIdAndModelName(schemaId, modelName);
  }

  public static String softDeleteModelVersionAliasRelsByModelIdAndVersion(
      @Param("modelId") Long modelId, @Param("modelVersion") Integer modelVersion) {
    return getProvider().softDeleteModelVersionAliasRelsByModelIdAndVersion(modelId, modelVersion);
  }

  public static String softDeleteModelVersionAliasRelsByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return getProvider().softDeleteModelVersionAliasRelsByModelIdAndAlias(modelId, alias);
  }

  public static String softDeleteModelVersionAliasRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteModelVersionAliasRelsBySchemaId(schemaId);
  }

  public static String softDeleteModelVersionAliasRelsByCatalogId(
      @Param("catalogId") Long catalogId) {
    return getProvider().softDeleteModelVersionAliasRelsByCatalogId(catalogId);
  }

  public static String softDeleteModelVersionAliasRelsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteModelVersionAliasRelsByMetalakeId(metalakeId);
  }

  public static String deleteModelVersionAliasRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteModelVersionAliasRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
