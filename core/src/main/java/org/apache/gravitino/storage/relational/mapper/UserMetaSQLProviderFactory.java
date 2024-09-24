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
import org.apache.gravitino.storage.relational.mapper.provider.base.UserMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.h2.UserMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.UserMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class UserMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, UserMetaBaseSQLProvider> USER_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new UserMetaMySQLProvider(),
          JDBCBackendType.H2, new UserMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new UserMetaPostgreSQLProvider());

  public static UserMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return USER_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserMetaMySQLProvider extends UserMetaBaseSQLProvider {}

  public static String selectUserIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String userName) {
    return getProvider().selectUserIdByMetalakeIdAndName(metalakeId, userName);
  }

  public static String selectUserMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name) {
    return getProvider().selectUserMetaByMetalakeIdAndName(metalakeId, name);
  }

  public static String insertUserMeta(@Param("userMeta") UserPO userPO) {
    return getProvider().insertUserMeta(userPO);
  }

  public static String insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO) {
    return getProvider().insertUserMetaOnDuplicateKeyUpdate(userPO);
  }

  public static String softDeleteUserMetaByUserId(@Param("userId") Long userId) {
    return getProvider().softDeleteUserMetaByUserId(userId);
  }

  public static String softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteUserMetasByMetalakeId(metalakeId);
  }

  public static String updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO) {
    return getProvider().updateUserMeta(newUserPO, oldUserPO);
  }

  public static String listUsersByRoleId(@Param("roleId") Long roleId) {
    return getProvider().listUsersByRoleId(roleId);
  }

  public static String listUserPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listUserPOsByMetalake(metalakeName);
  }

  public static String listExtendedUserPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().listExtendedUserPOsByMetalakeId(metalakeId);
  }

  public static String deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteUserMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
