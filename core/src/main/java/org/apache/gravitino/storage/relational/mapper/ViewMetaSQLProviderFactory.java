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
import org.apache.gravitino.storage.relational.mapper.provider.base.ViewMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.ViewMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class ViewMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, ViewMetaBaseSQLProvider> VIEW_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new ViewMetaMySQLProvider(),
          JDBCBackendType.H2, new ViewMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new ViewMetaPostgreSQLProvider());

  public static ViewMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return VIEW_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class ViewMetaMySQLProvider extends ViewMetaBaseSQLProvider {}

  static class ViewMetaH2Provider extends ViewMetaBaseSQLProvider {}

  public static String listViewPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listViewPOsBySchemaId(schemaId);
  }

  public static String listViewPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return getProvider().listViewPOsByFullQualifiedName(metalakeName, catalogName, schemaName);
  }

  public static String selectViewIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name) {
    return getProvider().selectViewIdBySchemaIdAndName(schemaId, name);
  }

  public static String listViewPOsByViewIds(@Param("viewIds") List<Long> viewIds) {
    return getProvider().listViewPOsByViewIds(viewIds);
  }

  public static String selectViewMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name) {
    return getProvider().selectViewMetaBySchemaIdAndName(schemaId, name);
  }

  public static String selectViewByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("viewName") String viewName) {
    return getProvider()
        .selectViewByFullQualifiedName(metalakeName, catalogName, schemaName, viewName);
  }

  public static String insertViewMeta(@Param("viewMeta") ViewPO viewPO) {
    return getProvider().insertViewMeta(viewPO);
  }

  public static String insertViewMetaOnDuplicateKeyUpdate(@Param("viewMeta") ViewPO viewPO) {
    return getProvider().insertViewMetaOnDuplicateKeyUpdate(viewPO);
  }

  public static String updateViewMeta(
      @Param("newViewMeta") ViewPO newViewPO, @Param("oldViewMeta") ViewPO oldViewPO) {
    return getProvider().updateViewMeta(newViewPO, oldViewPO);
  }

  public static String softDeleteViewMetasByViewId(@Param("viewId") Long viewId) {
    return getProvider().softDeleteViewMetasByViewId(viewId);
  }

  public static String softDeleteViewMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteViewMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteViewMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteViewMetasByCatalogId(catalogId);
  }

  public static String softDeleteViewMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteViewMetasBySchemaId(schemaId);
  }

  public static String deleteViewMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteViewMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
