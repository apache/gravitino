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

import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class GroupRoleRelSQLProvider {

  private static final Map<JDBCBackendType, GroupRoleRelBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new GroupRoleRelMySQLProvider(),
              JDBCBackendType.H2, new GroupRoleRelH2Provider(),
              JDBCBackendType.PG, new GroupRoleRelPGProvider());

  public static GroupRoleRelBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class GroupRoleRelMySQLProvider extends GroupRoleRelBaseProvider {}

  static class GroupRoleRelH2Provider extends GroupRoleRelBaseProvider {}

  static class GroupRoleRelPGProvider extends GroupRoleRelBaseProvider {

    @Override
    public String softDeleteGroupRoleRelByGroupId(Long groupId) {
      return "UPDATE "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE group_id = #{groupId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteGroupRoleRelByGroupAndRoles(Long groupId, List<Long> roleIds) {
      return "<script>"
          + "UPDATE "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE group_id = #{groupId} AND role_id in ("
          + "<foreach collection='roleIds' item='roleId' separator=','>"
          + "#{roleId}"
          + "</foreach>"
          + ") "
          + "AND deleted_at = 0"
          + "</script>";
    }

    @Override
    public String softDeleteGroupRoleRelByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE group_id IN (SELECT group_id FROM "
          + GROUP_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
          + " AND deleted_at = 0";
    }

    @Override
    public String softDeleteGroupRoleRelByRoleId(Long roleId) {
      return "UPDATE "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE role_id = #{roleId} AND deleted_at = 0";
    }
  }

  public String batchInsertGroupRoleRel(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return getProvider().batchInsertGroupRoleRel(groupRoleRelPOS);
  }

  public String batchInsertGroupRoleRelOnDuplicateKeyUpdate(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return getProvider().batchInsertGroupRoleRelOnDuplicateKeyUpdate(groupRoleRelPOS);
  }

  public String softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId) {
    return getProvider().softDeleteGroupRoleRelByGroupId(groupId);
  }

  public String softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds) {
    return getProvider().softDeleteGroupRoleRelByGroupAndRoles(groupId, roleIds);
  }

  public String softDeleteGroupRoleRelByMetalakeId(Long metalakeId) {
    return getProvider().softDeleteGroupRoleRelByMetalakeId(metalakeId);
  }

  public String softDeleteGroupRoleRelByRoleId(Long roleId) {
    return getProvider().softDeleteGroupRoleRelByRoleId(roleId);
  }

  public String deleteGroupRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteGroupRoleRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
