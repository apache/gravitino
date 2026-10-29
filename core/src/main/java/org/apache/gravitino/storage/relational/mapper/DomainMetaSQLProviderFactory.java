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
import org.apache.gravitino.storage.relational.mapper.provider.base.DomainMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.DomainMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects the SQL of the {@code domain_meta} table for the configured backend. */
public class DomainMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, DomainMetaBaseSQLProvider>
      DOMAIN_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new DomainMetaMySQLProvider(),
              JDBCBackendType.H2, new DomainMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new DomainMetaPostgreSQLProvider());

  /**
   * Returns the SQL provider of the configured backend.
   *
   * @return The SQL provider.
   */
  public static DomainMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return DOMAIN_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class DomainMetaMySQLProvider extends DomainMetaBaseSQLProvider {}

  static class DomainMetaH2Provider extends DomainMetaBaseSQLProvider {
    @Override
    public String selectDomainMetaByIdForShare(Long domainId) {
      // H2 has no shared row-lock syntax, so H2 backends fall back to an exclusive lock.
      return selectDomainMetaByIdForUpdate(domainId);
    }
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#listDomainPOsByMetalake}. */
  public static String listDomainPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listDomainPOsByMetalake(metalakeName);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaByMetalakeAndName}. */
  public static String selectDomainMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("domainName") String domainName) {
    return getProvider().selectDomainMetaByMetalakeAndName(metalakeName, domainName);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaByMetalakeIdAndName}. */
  public static String selectDomainMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName) {
    return getProvider().selectDomainMetaByMetalakeIdAndName(metalakeId, domainName);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaByMetalakeIdAndNameForUpdate}. */
  public static String selectDomainMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName) {
    return getProvider().selectDomainMetaByMetalakeIdAndNameForUpdate(metalakeId, domainName);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaById}. */
  public static String selectDomainMetaById(@Param("domainId") Long domainId) {
    return getProvider().selectDomainMetaById(domainId);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaByIdForUpdate}. */
  public static String selectDomainMetaByIdForUpdate(@Param("domainId") Long domainId) {
    return getProvider().selectDomainMetaByIdForUpdate(domainId);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#selectDomainMetaByIdForShare}. */
  public static String selectDomainMetaByIdForShare(@Param("domainId") Long domainId) {
    return getProvider().selectDomainMetaByIdForShare(domainId);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#listDomainPOsByDomainIds}. */
  public static String listDomainPOsByDomainIds(@Param("domainIds") List<Long> domainIds) {
    return getProvider().listDomainPOsByDomainIds(domainIds);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#listDomainPOsByMetalakeAndDomainNames}. */
  public static String listDomainPOsByMetalakeAndDomainNames(
      @Param("metalakeName") String metalakeName, @Param("domainNames") List<String> domainNames) {
    return getProvider().listDomainPOsByMetalakeAndDomainNames(metalakeName, domainNames);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#listDomainPOsByParentId}. */
  public static String listDomainPOsByParentId(@Param("parentId") Long parentId) {
    return getProvider().listDomainPOsByParentId(parentId);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#insertDomainMeta}. */
  public static String insertDomainMeta(@Param("domainMeta") DomainPO domainPO) {
    return getProvider().insertDomainMeta(domainPO);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#updateDomainMeta}. */
  public static String updateDomainMeta(
      @Param("newDomainMeta") DomainPO newDomainPO, @Param("oldDomainMeta") DomainPO oldDomainPO) {
    return getProvider().updateDomainMeta(newDomainPO, oldDomainPO);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#softDeleteDomainMetaByIdAndVersion}. */
  public static String softDeleteDomainMetaByIdAndVersion(
      @Param("domainId") Long domainId, @Param("currentVersion") Long currentVersion) {
    return getProvider().softDeleteDomainMetaByIdAndVersion(domainId, currentVersion);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#softDeleteDomainMetasByMetalakeId}. */
  public static String softDeleteDomainMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteDomainMetasByMetalakeId(metalakeId);
  }

  /** Delegates {@link DomainMetaBaseSQLProvider#deleteDomainMetasByLegacyTimeline}. */
  public static String deleteDomainMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteDomainMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
