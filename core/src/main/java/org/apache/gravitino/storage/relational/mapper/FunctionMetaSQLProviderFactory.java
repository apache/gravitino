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
import org.apache.gravitino.storage.relational.mapper.provider.base.FunctionMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.FunctionMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.FunctionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class FunctionMetaSQLProviderFactory {

  static class FunctionMetaMySQLProvider extends FunctionMetaBaseSQLProvider {}

  static class FunctionMetaH2Provider extends FunctionMetaBaseSQLProvider {}

  private static final Map<JDBCBackendType, FunctionMetaBaseSQLProvider>
      FUNCTION_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new FunctionMetaMySQLProvider(),
              JDBCBackendType.H2, new FunctionMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new FunctionMetaPostgreSQLProvider());

  public static FunctionMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return FUNCTION_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertFunctionMeta(@Param("functionMeta") FunctionPO functionPO) {
    return getProvider().insertFunctionMeta(functionPO);
  }

  public static String insertFunctionMetaOnDuplicateKeyUpdate(
      @Param("functionMeta") FunctionPO functionPO) {
    return getProvider().insertFunctionMetaOnDuplicateKeyUpdate(functionPO);
  }

  public static String listFunctionPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listFunctionPOsBySchemaId(schemaId);
  }

  public static String listFunctionPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return getProvider().listFunctionPOsByFullQualifiedName(metalakeName, catalogName, schemaName);
  }

  public static String selectFunctionMetaByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("functionName") String functionName) {
    return getProvider()
        .selectFunctionMetaByFullQualifiedName(metalakeName, catalogName, schemaName, functionName);
  }

  public static String selectFunctionMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("functionName") String functionName) {
    return getProvider().selectFunctionMetaBySchemaIdAndName(schemaId, functionName);
  }

  public static String softDeleteFunctionMetaByFunctionId(@Param("functionId") Long functionId) {
    return getProvider().softDeleteFunctionMetaByFunctionId(functionId);
  }

  public static String softDeleteFunctionMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteFunctionMetasByCatalogId(catalogId);
  }

  public static String softDeleteFunctionMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteFunctionMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteFunctionMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteFunctionMetasBySchemaId(schemaId);
  }

  public static String deleteFunctionMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteFunctionMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String updateFunctionMeta(
      @Param("newFunctionMeta") FunctionPO newFunctionPO,
      @Param("oldFunctionMeta") FunctionPO oldFunctionPO) {
    return getProvider().updateFunctionMeta(newFunctionPO, oldFunctionPO);
  }
}
