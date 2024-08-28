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

public class UserRoleRelSQLProvider {

  private static final Map<JDBCBackendType, UserRoleRelBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new UserRoleRelMySQLProvider(),
              JDBCBackendType.H2, new UserRoleRelH2Provider(),
              JDBCBackendType.PG, new UserRoleRelPGProvider());

  public static UserRoleRelBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserRoleRelMySQLProvider extends UserRoleRelBaseProvider {}

  static class UserRoleRelH2Provider extends UserRoleRelBaseProvider {}

  static class UserRoleRelPGProvider extends UserRoleRelBaseProvider {

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
  }

  public String batchInsertUserRoleRel(@Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRel(userRoleRelPOs);
  }

  public String batchInsertUserRoleRelOnDuplicateKeyUpdate(
      @Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return getProvider().batchInsertUserRoleRelOnDuplicateKeyUpdate(userRoleRelPOs);
  }

  public String softDeleteUserRoleRelByUserId(@Param("userId") Long userId) {
    return getProvider().softDeleteUserRoleRelByUserId(userId);
  }

  public String softDeleteUserRoleRelByUserAndRoles(
      @Param("userId") Long userId, @Param("roleIds") List<Long> roleIds) {
    return getProvider().softDeleteUserRoleRelByUserAndRoles(userId, roleIds);
  }

  public String softDeleteUserRoleRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteUserRoleRelByMetalakeId(metalakeId);
  }

  public String softDeleteUserRoleRelByRoleId(@Param("roleId") Long roleId) {
    return getProvider().softDeleteUserRoleRelByRoleId(roleId);
  }

  public String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteUserRoleRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
