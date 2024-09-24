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
import org.apache.gravitino.storage.relational.mapper.provider.base.UserRoleRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.UserRoleRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelSQLProviderFactory {

  private static final Map<JDBCBackendType, UserRoleRelBaseSQLProvider>
      USER_ROLE_RELATIONAL_SQL_PROVIDER_MAP =
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
    return USER_ROLE_RELATIONAL_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class UserRoleRelMySQLProvider extends UserRoleRelBaseSQLProvider {}

  static class UserRoleRelH2Provider extends UserRoleRelBaseSQLProvider {}

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
