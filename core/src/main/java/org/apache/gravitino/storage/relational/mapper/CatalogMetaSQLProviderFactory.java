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

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class CatalogMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, CatalogMetaBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
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
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class CatalogMetaMySQLProvider extends CatalogMetaBaseSQLProvider {}

  static class CatalogMetaH2Provider extends CatalogMetaBaseSQLProvider {}

  static class CatalogMetaPostgreSQLProvider extends CatalogMetaBaseSQLProvider {

    @Override
    public String softDeleteCatalogMetasByCatalogId(Long catalogId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteCatalogMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String insertCatalogMetaOnDuplicateKeyUpdate(CatalogPO catalogPO) {
      return "INSERT INTO "
          + TABLE_NAME
          + "(catalog_id, catalog_name, metalake_id,"
          + " type, provider, catalog_comment, properties, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{catalogMeta.catalogId},"
          + " #{catalogMeta.catalogName},"
          + " #{catalogMeta.metalakeId},"
          + " #{catalogMeta.type},"
          + " #{catalogMeta.provider},"
          + " #{catalogMeta.catalogComment},"
          + " #{catalogMeta.properties},"
          + " #{catalogMeta.auditInfo},"
          + " #{catalogMeta.currentVersion},"
          + " #{catalogMeta.lastVersion},"
          + " #{catalogMeta.deletedAt}"
          + " )"
          + " ON CONFLICT(catalog_id) DO UPDATE SET"
          + " catalog_name = #{catalogMeta.catalogName},"
          + " metalake_id = #{catalogMeta.metalakeId},"
          + " type = #{catalogMeta.type},"
          + " provider = #{catalogMeta.provider},"
          + " catalog_comment = #{catalogMeta.catalogComment},"
          + " properties = #{catalogMeta.properties},"
          + " audit_info = #{catalogMeta.auditInfo},"
          + " current_version = #{catalogMeta.currentVersion},"
          + " last_version = #{catalogMeta.lastVersion},"
          + " deleted_at = #{catalogMeta.deletedAt}";
    }
  }

  public static String listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().listCatalogPOsByMetalakeId(metalakeId);
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
