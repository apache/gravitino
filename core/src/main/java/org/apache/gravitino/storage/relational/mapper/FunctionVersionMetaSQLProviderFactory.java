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
import org.apache.gravitino.storage.relational.mapper.provider.base.FunctionVersionMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.FunctionVersionMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.FunctionVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class FunctionVersionMetaSQLProviderFactory {

  static class FunctionVersionMetaMySQLProvider extends FunctionVersionMetaBaseSQLProvider {}

  static class FunctionVersionMetaH2Provider extends FunctionVersionMetaBaseSQLProvider {}

  private static final Map<JDBCBackendType, FunctionVersionMetaBaseSQLProvider>
      FUNCTION_VERSION_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new FunctionVersionMetaMySQLProvider(),
              JDBCBackendType.H2, new FunctionVersionMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new FunctionVersionMetaPostgreSQLProvider());

  public static FunctionVersionMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return FUNCTION_VERSION_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertFunctionVersionMeta(
      @Param("functionVersionMeta") FunctionVersionPO functionVersionPO) {
    return getProvider().insertFunctionVersionMeta(functionVersionPO);
  }

  public static String insertFunctionVersionMetaOnDuplicateKeyUpdate(
      @Param("functionVersionMeta") FunctionVersionPO functionVersionPO) {
    return getProvider().insertFunctionVersionMetaOnDuplicateKeyUpdate(functionVersionPO);
  }

  public static String softDeleteFunctionVersionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteFunctionVersionMetasBySchemaId(schemaId);
  }

  public static String softDeleteFunctionVersionMetasByCatalogId(
      @Param("catalogId") Long catalogId) {
    return getProvider().softDeleteFunctionVersionMetasByCatalogId(catalogId);
  }

  public static String softDeleteFunctionVersionMetasByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteFunctionVersionMetasByMetalakeId(metalakeId);
  }

  public static String deleteFunctionVersionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteFunctionVersionMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String selectFunctionVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return getProvider().selectFunctionVersionsByRetentionCount(versionRetentionCount);
  }

  public static String softDeleteFunctionVersionsByRetentionLine(
      @Param("functionId") Long functionId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return getProvider()
        .softDeleteFunctionVersionsByRetentionLine(functionId, versionRetentionLine, limit);
  }

  public static String softDeleteFunctionVersionsByFunctionId(
      @Param("functionId") Long functionId) {
    return getProvider().softDeleteFunctionVersionsByFunctionId(functionId);
  }
}
