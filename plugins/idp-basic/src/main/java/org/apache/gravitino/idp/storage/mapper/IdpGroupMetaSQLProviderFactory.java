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

package org.apache.gravitino.idp.storage.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaSQLProviderFactory {
  private static final IdpGroupMetaBaseSQLProvider IDP_GROUP_META_BASE_PROVIDER =
      new IdpGroupMetaBaseSQLProvider();
  private static final IdpGroupMetaBaseSQLProvider IDP_GROUP_META_H2_PROVIDER =
      new IdpGroupMetaH2Provider();
  private static final IdpGroupMetaBaseSQLProvider IDP_GROUP_META_POSTGRESQL_PROVIDER =
      new IdpGroupMetaPostgreSQLProvider();

  private static final Map<JDBCBackendType, IdpGroupMetaBaseSQLProvider>
      IDP_GROUP_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, IDP_GROUP_META_BASE_PROVIDER,
              JDBCBackendType.H2, IDP_GROUP_META_H2_PROVIDER,
              JDBCBackendType.POSTGRESQL, IDP_GROUP_META_POSTGRESQL_PROVIDER);

  static IdpGroupMetaBaseSQLProvider getProvider(String databaseId) {
    if (databaseId == null) {
      throw new IllegalStateException(
          "MyBatis databaseId is not configured for IdP group SQL providers.");
    }

    try {
      JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
      IdpGroupMetaBaseSQLProvider provider = IDP_GROUP_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
      if (provider != null) {
        return provider;
      }

      throw new IllegalStateException(
          String.format(
              "No IdP group SQL provider registered for backend %s (databaseId: %s)",
              jdbcBackendType, databaseId));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format(
              "Unsupported IdP group SQL provider databaseId: %s, supported backends: %s",
              databaseId, IDP_GROUP_META_SQL_PROVIDER_MAP.keySet()),
          e);
    }
  }

  public static IdpGroupMetaBaseSQLProvider h2Provider() {
    return IDP_GROUP_META_H2_PROVIDER;
  }

  public static IdpGroupMetaBaseSQLProvider mysqlProvider() {
    return IDP_GROUP_META_BASE_PROVIDER;
  }

  public static IdpGroupMetaBaseSQLProvider postgresqlProvider() {
    return IDP_GROUP_META_POSTGRESQL_PROVIDER;
  }

  public static String selectIdpGroup(@Param("groupname") String groupname) {
    return getProvider(currentDatabaseId()).selectIdpGroup(groupname);
  }

  public static String insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return getProvider(currentDatabaseId()).insertIdpGroup(groupPO);
  }

  public static String softDeleteIdpGroup(@Param("groupId") Long groupId) {
    return getProvider(currentDatabaseId()).softDeleteIdpGroup(groupId);
  }

  public static String deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider(currentDatabaseId())
        .deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }

  private static String currentDatabaseId() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getDatabaseId();
  }
}
