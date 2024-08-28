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

import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class UserMetaSQLProvider {

  private static final Map<JDBCBackendType, UserMetaBaseProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new UserMetaMySQLProvider(),
          JDBCBackendType.H2, new UserMetaH2Provider(),
          JDBCBackendType.PG, new UserMetaPostgreSQLProvider());

  public static UserMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserMetaMySQLProvider extends UserMetaBaseProvider {}

  static class UserMetaH2Provider extends UserMetaBaseProvider {}

  static class UserMetaPostgreSQLProvider extends UserMetaBaseProvider {

    @Override
    public String softDeleteUserMetaByUserId(Long userId) {
      return "UPDATE "
          + USER_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE user_id = #{userId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteUserMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + USER_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String insertUserMetaOnDuplicateKeyUpdate(UserPO userPO) {
      return "INSERT INTO "
          + USER_TABLE_NAME
          + "(user_id, user_name,"
          + "metalake_id, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{userMeta.userId},"
          + " #{userMeta.userName},"
          + " #{userMeta.metalakeId},"
          + " #{userMeta.auditInfo},"
          + " #{userMeta.currentVersion},"
          + " #{userMeta.lastVersion},"
          + " #{userMeta.deletedAt}"
          + " )"
          + " ON CONFLICT(user_id) DO UPDATE SET"
          + " user_name = #{userMeta.userName},"
          + " metalake_id = #{userMeta.metalakeId},"
          + " audit_info = #{userMeta.auditInfo},"
          + " current_version = #{userMeta.currentVersion},"
          + " last_version = #{userMeta.lastVersion},"
          + " deleted_at = #{userMeta.deletedAt}";
    }
  }

  public String selectUserIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String userName) {
    return getProvider().selectUserIdByMetalakeIdAndName(metalakeId, userName);
  }

  public String selectUserMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name) {
    return getProvider().selectUserMetaByMetalakeIdAndName(metalakeId, name);
  }

  public String insertUserMeta(@Param("userMeta") UserPO userPO) {
    return getProvider().insertUserMeta(userPO);
  }

  public String insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO) {
    return getProvider().insertUserMetaOnDuplicateKeyUpdate(userPO);
  }

  public String softDeleteUserMetaByUserId(@Param("userId") Long userId) {
    return getProvider().softDeleteUserMetaByUserId(userId);
  }

  public String softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteUserMetasByMetalakeId(metalakeId);
  }

  public String updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO) {
    return getProvider().updateUserMeta(newUserPO, oldUserPO);
  }

  public String listUsersByRoleId(@Param("roleId") Long roleId) {
    return getProvider().listUsersByRoleId(roleId);
  }

  public String deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteUserMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
