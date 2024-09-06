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

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class RoleMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, RoleMetaBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new RoleMetaMySQLProvider(),
              JDBCBackendType.H2, new RoleMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new RoleMetaPostgreSQLProvider());

  public static RoleMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class RoleMetaMySQLProvider extends RoleMetaBaseSQLProvider {}

  static class RoleMetaH2Provider extends RoleMetaBaseSQLProvider {}

  static class RoleMetaPostgreSQLProvider extends RoleMetaBaseSQLProvider {

    @Override
    public String softDeleteRoleMetaByRoleId(Long roleId) {
      return "UPDATE "
          + ROLE_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE role_id = #{roleId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteRoleMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + ROLE_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String insertRoleMetaOnDuplicateKeyUpdate(RolePO rolePO) {
      return "INSERT INTO "
          + ROLE_TABLE_NAME
          + "(role_id, role_name,"
          + " metalake_id, properties,"
          + " audit_info, current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{roleMeta.roleId},"
          + " #{roleMeta.roleName},"
          + " #{roleMeta.metalakeId},"
          + " #{roleMeta.properties},"
          + " #{roleMeta.auditInfo},"
          + " #{roleMeta.currentVersion},"
          + " #{roleMeta.lastVersion},"
          + " #{roleMeta.deletedAt}"
          + " ) ON CONFLICT (role_id) DO UPDATE SET"
          + " role_name = #{roleMeta.roleName},"
          + " metalake_id = #{roleMeta.metalakeId},"
          + " properties = #{roleMeta.properties},"
          + " audit_info = #{roleMeta.auditInfo},"
          + " current_version = #{roleMeta.currentVersion},"
          + " last_version = #{roleMeta.lastVersion},"
          + " deleted_at = #{roleMeta.deletedAt}";
    }
  }

  public static String selectRoleMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String roleName) {
    return getProvider().selectRoleMetaByMetalakeIdAndName(metalakeId, roleName);
  }

  public static String selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name) {
    return getProvider().selectRoleIdByMetalakeIdAndName(metalakeId, name);
  }

  public static String listRolesByUserId(@Param("userId") Long userId) {
    return getProvider().listRolesByUserId(userId);
  }

  public static String listRolesByGroupId(Long groupId) {
    return getProvider().listRolesByGroupId(groupId);
  }

  public static String listRolesByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId, @Param("metadataObjectType") String type) {
    return getProvider().listRolesByMetadataObjectIdAndType(metadataObjectId, type);
  }

  public static String insertRoleMeta(@Param("roleMeta") RolePO rolePO) {
    return getProvider().insertRoleMeta(rolePO);
  }

  public static String insertRoleMetaOnDuplicateKeyUpdate(@Param("roleMeta") RolePO rolePO) {
    return getProvider().insertRoleMetaOnDuplicateKeyUpdate(rolePO);
  }

  public static String softDeleteRoleMetaByRoleId(Long roleId) {
    return getProvider().softDeleteRoleMetaByRoleId(roleId);
  }

  public static String softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteRoleMetasByMetalakeId(metalakeId);
  }

  public static String deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteRoleMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
