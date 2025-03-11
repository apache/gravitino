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
import org.apache.gravitino.storage.relational.mapper.provider.base.CatalogMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.CatalogMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, CatalogMetaBaseSQLProvider>
      CATALOG_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new CatalogMetaMySQLProvider(),
              JDBCBackendType.H2, new CatalogMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new CatalogMetaPostgreSQLProvider());

  public static CatalogMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return CATALOG_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class CatalogMetaMySQLProvider extends CatalogMetaBaseSQLProvider {}

  static class CatalogMetaH2Provider extends CatalogMetaBaseSQLProvider {}

  public static String listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().listCatalogPOsByMetalakeId(metalakeId);
  }

  public static String listCatalogPOsByCatalogIds(@Param("catalogIds") List<Long> catalogIds) {
    return getProvider().listCatalogPOsByCatalogIds(catalogIds);
  }

  public static String selectCatalogIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name) {
    return getProvider().selectCatalogIdByMetalakeIdAndName(metalakeId, name);
  }

  public static String selectCatalogMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name) {
    return getProvider().selectCatalogMetaByMetalakeIdAndName(metalakeId, name);
  }

  public static String selectCatalogMetaById(@Param("catalogId") Long catalogId) {
    return getProvider().selectCatalogMetaById(catalogId);
  }

  public static String insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO) {
    return getProvider().insertCatalogMeta(catalogPO);
  }

  public static String insertCatalogMetaOnDuplicateKeyUpdate(
      @Param("catalogMeta") CatalogPO catalogPO) {
    return getProvider().insertCatalogMetaOnDuplicateKeyUpdate(catalogPO);
  }

  public static String updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO) {
    return getProvider().updateCatalogMeta(newCatalogPO, oldCatalogPO);
  }

  public static String softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteCatalogMetasByCatalogId(catalogId);
  }

  public static String softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteCatalogMetasByMetalakeId(metalakeId);
  }

  public static String deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteCatalogMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
