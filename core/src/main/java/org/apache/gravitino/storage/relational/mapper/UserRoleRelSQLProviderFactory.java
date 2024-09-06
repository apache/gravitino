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

import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelSQLProviderFactory {

  private static final Map<JDBCBackendType, UserRoleRelBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new UserRoleRelMySQLProvider(),
              JDBCBackendType.H2, new UserRoleRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new UserRoleRelPostgreSQLProvider());

  public static UserRoleRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserRoleRelMySQLProvider extends UserRoleRelBaseSQLProvider {}

  static class UserRoleRelH2Provider extends UserRoleRelBaseSQLProvider {}

  static class UserRoleRelPostgreSQLProvider extends UserRoleRelBaseSQLProvider {

    @Override
    public String softDeleteUserRoleRelByUserId(Long userId) {
      return "UPDATE "
          + USER_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE user_id = #{userId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteUserRoleRelByUserAndRoles(Long userId, List<Long> roleIds) {
      return "<script>"
          + "UPDATE "
          + USER_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE user_id = #{userId} AND role_id in ("
          + "<foreach collection='roleIds' item='roleId' separator=','>"
          + "#{roleId}"
          + "</foreach>"
          + ") "
          + "AND deleted_at = 0"
          + "</script>";
    }

    @Override
    public String softDeleteUserRoleRelByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + USER_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE user_id IN (SELECT user_id FROM "
          + USER_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
          + " AND deleted_at = 0";
    }

    @Override
    public String softDeleteUserRoleRelByRoleId(Long roleId) {
      return "UPDATE "
          + USER_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE role_id = #{roleId} AND deleted_at = 0";
    }

    @Override
    public String batchInsertUserRoleRelOnDuplicateKeyUpdate(List<UserRoleRelPO> userRoleRelPOs) {
      return "<script>"
          + "INSERT INTO "
          + USER_ROLE_RELATION_TABLE_NAME
          + "(user_id, role_id,"
          + " audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES "
          + "<foreach collection='userRoleRels' item='item' separator=','>"
          + "(#{item.userId},"
          + " #{item.roleId},"
          + " #{item.auditInfo},"
          + " #{item.currentVersion},"
          + " #{item.lastVersion},"
          + " #{item.deletedAt})"
          + "</foreach>"
          + " ON CONFLICT (user_id, role_id, deleted_at) DO UPDATE SET"
          + " user_id = VALUES(user_id),"
          + " role_id = VALUES(role_id),"
          + " audit_info = VALUES(audit_info),"
          + " current_version = VALUES(current_version),"
          + " last_version = VALUES(last_version),"
          + " deleted_at = VALUES(deleted_at)"
          + "</script>";
    }
  }

  public static String batchInsertUserRoleRel(
      @Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRel(userRoleRelPOs);
  }

  public static String batchInsertUserRoleRelOnDuplicateKeyUpdate(
      @Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRelOnDuplicateKeyUpdate(userRoleRelPOs);
  }

  public static String softDeleteUserRoleRelByUserId(@Param("userId") Long userId) {
    return getProvider().softDeleteUserRoleRelByUserId(userId);
  }

  public static String softDeleteUserRoleRelByUserAndRoles(
      @Param("userId") Long userId, @Param("roleIds") List<Long> roleIds) {
    return getProvider().softDeleteUserRoleRelByUserAndRoles(userId, roleIds);
  }

  public static String softDeleteUserRoleRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteUserRoleRelByMetalakeId(metalakeId);
  }

  public static String softDeleteUserRoleRelByRoleId(@Param("roleId") Long roleId) {
    return getProvider().softDeleteUserRoleRelByRoleId(roleId);
  }

  public static String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
