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
import org.apache.gravitino.storage.relational.mapper.provider.base.GroupRoleRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.GroupRoleRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class GroupRoleRelSQLProviderFactory {

  private static final Map<JDBCBackendType, GroupRoleRelBaseSQLProvider>
      GROUP_ROLE_RELATION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new GroupRoleRelMySQLProvider(),
              JDBCBackendType.H2, new GroupRoleRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new GroupRoleRelPostgreSQLProvider());

  public static GroupRoleRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return GROUP_ROLE_RELATION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class GroupRoleRelMySQLProvider extends GroupRoleRelBaseSQLProvider {}

  static class GroupRoleRelH2Provider extends GroupRoleRelBaseSQLProvider {}

  public static String batchInsertGroupRoleRel(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return getProvider().batchInsertGroupRoleRel(groupRoleRelPOS);
  }

  public static String batchInsertGroupRoleRelOnDuplicateKeyUpdate(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return getProvider().batchInsertGroupRoleRelOnDuplicateKeyUpdate(groupRoleRelPOS);
  }

  public static String softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId) {
    return getProvider().softDeleteGroupRoleRelByGroupId(groupId);
  }

  public static String softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds) {
    return getProvider().softDeleteGroupRoleRelByGroupAndRoles(groupId, roleIds);
  }

  public static String softDeleteGroupRoleRelByMetalakeId(Long metalakeId) {
    return getProvider().softDeleteGroupRoleRelByMetalakeId(metalakeId);
  }

  public static String softDeleteGroupRoleRelByRoleId(Long roleId) {
    return getProvider().softDeleteGroupRoleRelByRoleId(roleId);
  }

  public static String deleteGroupRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteGroupRoleRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
