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
import org.apache.gravitino.storage.relational.mapper.provider.base.ViewVersionInfoBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.ViewVersionInfoPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ViewVersionInfoPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class ViewVersionInfoSQLProviderFactory {

  static class ViewVersionInfoMySQLProvider extends ViewVersionInfoBaseSQLProvider {}

  static class ViewVersionInfoH2Provider extends ViewVersionInfoBaseSQLProvider {}

  private static final Map<JDBCBackendType, ViewVersionInfoBaseSQLProvider>
      VIEW_VERSION_INFO_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new ViewVersionInfoMySQLProvider(),
              JDBCBackendType.H2, new ViewVersionInfoH2Provider(),
              JDBCBackendType.POSTGRESQL, new ViewVersionInfoPostgreSQLProvider());

  public static ViewVersionInfoBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return VIEW_VERSION_INFO_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertViewVersionInfo(
      @Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO) {
    return getProvider().insertViewVersionInfo(viewVersionInfoPO);
  }

  public static String insertViewVersionInfoOnDuplicateKeyUpdate(
      @Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO) {
    return getProvider().insertViewVersionInfoOnDuplicateKeyUpdate(viewVersionInfoPO);
  }

  public static String selectViewVersionInfoByViewIdAndVersion(
      @Param("viewId") Long viewId, @Param("version") Integer version) {
    return getProvider().selectViewVersionInfoByViewIdAndVersion(viewId, version);
  }

  public static String softDeleteViewVersionsByViewId(@Param("viewId") Long viewId) {
    return getProvider().softDeleteViewVersionsByViewId(viewId);
  }

  public static String softDeleteViewVersionsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteViewVersionsBySchemaId(schemaId);
  }

  public static String softDeleteViewVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteViewVersionsByCatalogId(catalogId);
  }

  public static String softDeleteViewVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteViewVersionsByMetalakeId(metalakeId);
  }

  public static String deleteViewVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteViewVersionsByLegacyTimeline(legacyTimeline, limit);
  }
}
