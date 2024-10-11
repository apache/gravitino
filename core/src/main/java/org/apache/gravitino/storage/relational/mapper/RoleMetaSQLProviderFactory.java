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
import org.apache.gravitino.storage.relational.mapper.provider.base.RoleMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.RoleMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class RoleMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, RoleMetaBaseSQLProvider> ROLE_META_SQL_PROVIDER_MAP =
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
    return ROLE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class RoleMetaMySQLProvider extends RoleMetaBaseSQLProvider {}

  static class RoleMetaH2Provider extends RoleMetaBaseSQLProvider {}

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

  public static String listRolePOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listRolePOsByMetalake(metalakeName);
  }

  public static String insertRoleMeta(@Param("roleMeta") RolePO rolePO) {
    return getProvider().insertRoleMeta(rolePO);
  }

  public static String insertRoleMetaOnDuplicateKeyUpdate(@Param("roleMeta") RolePO rolePO) {
    return getProvider().insertRoleMetaOnDuplicateKeyUpdate(rolePO);
  }

  public static String updateRoleMeta(
      @Param("newRoleMeta") RolePO newRolePO, @Param("oldRoleMeta") RolePO oldRolePO) {
    return getProvider().updateRoleMeta(newRolePO, oldRolePO);
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
