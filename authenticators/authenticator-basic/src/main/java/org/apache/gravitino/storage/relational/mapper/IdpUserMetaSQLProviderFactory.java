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
import org.apache.gravitino.storage.relational.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.h2.IdpUserMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.IdpUserMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, IdpUserMetaBaseSQLProvider>
      IDP_USER_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new IdpUserMetaMySQLProvider(),
              JDBCBackendType.H2, new IdpUserMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new IdpUserMetaPostgreSQLProvider());

  public static IdpUserMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return IDP_USER_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class IdpUserMetaMySQLProvider extends IdpUserMetaBaseSQLProvider {}

  public static String selectLocalUser(@Param("userName") String userName) {
    return getProvider().selectLocalUser(userName);
  }

  public static String selectLocalUsers(@Param("userNames") List<String> userNames) {
    return getProvider().selectLocalUsers(userNames);
  }

  public static String insertLocalUser(@Param("userMeta") IdpUserPO userPO) {
    return getProvider().insertLocalUser(userPO);
  }

  public static String updateLocalUserPassword(
      @Param("userId") Long userId,
      @Param("passwordHash") String passwordHash,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") Long currentVersion,
      @Param("newCurrentVersion") Long newCurrentVersion,
      @Param("newLastVersion") Long newLastVersion) {
    return getProvider()
        .updateLocalUserPassword(
            userId, passwordHash, auditInfo, currentVersion, newCurrentVersion, newLastVersion);
  }

  public static String softDeleteLocalUser(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return getProvider().softDeleteLocalUser(userId, deletedAt, auditInfo);
  }

  public static String truncateLocalUserMeta() {
    return getProvider().truncateLocalUserMeta();
  }
}
