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

package org.apache.gravitino.idp.basic.storage.relational.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.idp.basic.storage.relational.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.gravitino.idp.basic.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaSQLProviderFactory {
  private static final IdpUserMetaBaseSQLProvider IDP_USER_META_H2_PROVIDER =
      new IdpUserMetaH2Provider();
  private static final IdpUserMetaBaseSQLProvider IDP_USER_META_MYSQL_PROVIDER =
      new IdpUserMetaMySQLProvider();
  private static final IdpUserMetaBaseSQLProvider IDP_USER_META_POSTGRESQL_PROVIDER =
      new IdpUserMetaPostgreSQLProvider();

  private static final Map<JDBCBackendType, IdpUserMetaBaseSQLProvider>
      IDP_USER_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, IDP_USER_META_MYSQL_PROVIDER,
              JDBCBackendType.H2, IDP_USER_META_H2_PROVIDER,
              JDBCBackendType.POSTGRESQL, IDP_USER_META_POSTGRESQL_PROVIDER);

  public static IdpUserMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    return getProvider(databaseId);
  }

  static IdpUserMetaBaseSQLProvider getProvider(String databaseId) {
    if (databaseId == null) {
      throw new IllegalStateException(
          "MyBatis databaseId is not configured for IdP user SQL providers.");
    }

    JDBCBackendType jdbcBackendType;
    try {
      jdbcBackendType = JDBCBackendType.fromString(databaseId);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format(
              "Unsupported IdP user SQL provider databaseId: %s, supported backends: %s",
              databaseId, IDP_USER_META_SQL_PROVIDER_MAP.keySet()),
          e);
    }

    return getProvider(jdbcBackendType, databaseId, IDP_USER_META_SQL_PROVIDER_MAP);
  }

  static IdpUserMetaBaseSQLProvider getProvider(
      JDBCBackendType jdbcBackendType,
      String databaseId,
      Map<JDBCBackendType, IdpUserMetaBaseSQLProvider> providerMap) {
    IdpUserMetaBaseSQLProvider provider = providerMap.get(jdbcBackendType);
    if (provider == null) {
      throw new IllegalStateException(
          String.format(
              "No IdP user SQL provider registered for backend %s (databaseId: %s)",
              jdbcBackendType, databaseId));
    }

    return provider;
  }

  public static IdpUserMetaBaseSQLProvider h2Provider() {
    return IDP_USER_META_H2_PROVIDER;
  }

  public static IdpUserMetaBaseSQLProvider mysqlProvider() {
    return IDP_USER_META_MYSQL_PROVIDER;
  }

  public static IdpUserMetaBaseSQLProvider postgresqlProvider() {
    return IDP_USER_META_POSTGRESQL_PROVIDER;
  }

  public static String selectIdpUser(@Param("username") String username) {
    return getProvider().selectIdpUser(username);
  }

  public static String selectIdpUsers(@Param("usernames") List<String> usernames) {
    return getProvider().selectIdpUsers(usernames);
  }

  public static String insertIdpUser(@Param("userMeta") IdpUserPO userPO) {
    return getProvider().insertIdpUser(userPO);
  }

  public static String updateIdpUserPassword(
      @Param("userId") Long userId, @Param("passwordHash") String passwordHash) {
    return getProvider().updateIdpUserPassword(userId, passwordHash);
  }

  public static String softDeleteIdpUser(@Param("userId") Long userId) {
    return getProvider().softDeleteIdpUser(userId);
  }

  public static String deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteIdpUserMetasByLegacyTimeline(legacyTimeline, limit);
  }

  static class IdpUserMetaH2Provider extends IdpUserMetaBaseSQLProvider {

    @Override
    protected String currentTimeMillisExpression() {
      return "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())";
    }
  }

  static class IdpUserMetaMySQLProvider extends IdpUserMetaBaseSQLProvider {

    @Override
    protected String currentTimeMillisExpression() {
      return "(UNIX_TIMESTAMP() * 1000.0)";
    }
  }

  static class IdpUserMetaPostgreSQLProvider extends IdpUserMetaBaseSQLProvider {

    @Override
    protected String currentTimeMillisExpression() {
      return "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";
    }

    @Override
    public String deleteIdpUserMetasByLegacyTimeline(
        @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
      return "DELETE FROM "
          + IdpUserMetaMapper.IDP_USER_TABLE_NAME
          + " WHERE user_id IN (SELECT user_id FROM "
          + IdpUserMetaMapper.IDP_USER_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
    }
  }
}
