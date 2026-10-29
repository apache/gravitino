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
import org.apache.gravitino.storage.relational.mapper.provider.base.DomainRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.DomainRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects the SQL of the {@code domain_rel} table for the configured backend. */
public class DomainRelSQLProviderFactory {

  private static final Map<JDBCBackendType, DomainRelBaseSQLProvider> DOMAIN_REL_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new DomainRelMySQLProvider(),
          JDBCBackendType.H2, new DomainRelH2Provider(),
          JDBCBackendType.POSTGRESQL, new DomainRelPostgreSQLProvider());

  /**
   * Returns the SQL provider of the configured backend.
   *
   * @return The SQL provider.
   */
  public static DomainRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return DOMAIN_REL_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class DomainRelMySQLProvider extends DomainRelBaseSQLProvider {}

  static class DomainRelH2Provider extends DomainRelBaseSQLProvider {}

  /** Delegates {@link DomainRelBaseSQLProvider#selectDomainRelByChildId}. */
  public static String selectDomainRelByChildId(@Param("childId") Long childId) {
    return getProvider().selectDomainRelByChildId(childId);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#listDomainRelsByChildIds}. */
  public static String listDomainRelsByChildIds(@Param("childIds") List<Long> childIds) {
    return getProvider().listDomainRelsByChildIds(childIds);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#countDomainRelsByParentId}. */
  public static String countDomainRelsByParentId(@Param("parentId") Long parentId) {
    return getProvider().countDomainRelsByParentId(parentId);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#insertDomainRel}. */
  public static String insertDomainRel(@Param("domainRel") DomainRelPO domainRelPO) {
    return getProvider().insertDomainRel(domainRelPO);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#updateDomainRel}. */
  public static String updateDomainRel(
      @Param("newDomainRel") DomainRelPO newDomainRelPO,
      @Param("oldDomainRel") DomainRelPO oldDomainRelPO) {
    return getProvider().updateDomainRel(newDomainRelPO, oldDomainRelPO);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#softDeleteDomainRelByChildId}. */
  public static String softDeleteDomainRelByChildId(@Param("childId") Long childId) {
    return getProvider().softDeleteDomainRelByChildId(childId);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#softDeleteDomainRelsByMetalakeId}. */
  public static String softDeleteDomainRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteDomainRelsByMetalakeId(metalakeId);
  }

  /** Delegates {@link DomainRelBaseSQLProvider#deleteDomainRelsByLegacyTimeline}. */
  public static String deleteDomainRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteDomainRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
