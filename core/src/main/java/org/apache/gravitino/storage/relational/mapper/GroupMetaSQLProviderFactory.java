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
import org.apache.gravitino.storage.relational.mapper.provider.base.GroupMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.h2.GroupMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.GroupMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class GroupMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, GroupMetaBaseSQLProvider> GROUP_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new GroupMetaMySQLProvider(),
          JDBCBackendType.H2, new GroupMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new GroupMetaPostgreSQLProvider());

  public static GroupMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return GROUP_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class GroupMetaMySQLProvider extends GroupMetaBaseSQLProvider {}

  public static String selectGroupIdBySchemaIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name) {
    return getProvider().selectGroupIdBySchemaIdAndName(metalakeId, name);
  }

  public static String selectGroupMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name) {
    return getProvider().selectGroupMetaByMetalakeIdAndName(metalakeId, name);
  }

  public static String insertGroupMeta(@Param("groupMeta") GroupPO groupPO) {
    return getProvider().insertGroupMeta(groupPO);
  }

  public static String insertGroupMetaOnDuplicateKeyUpdate(@Param("groupMeta") GroupPO groupPO) {
    return getProvider().insertGroupMetaOnDuplicateKeyUpdate(groupPO);
  }

  public static String softDeleteGroupMetaByGroupId(@Param("groupId") Long groupId) {
    return getProvider().softDeleteGroupMetaByGroupId(groupId);
  }

  public static String softDeleteGroupMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteGroupMetasByMetalakeId(metalakeId);
  }

  public static String updateGroupMeta(
      @Param("newGroupMeta") GroupPO newGroupPO, @Param("oldGroupMeta") GroupPO oldGroupPO) {
    return getProvider().updateGroupMeta(newGroupPO, oldGroupPO);
  }

  public static String listGroupsByRoleId(@Param("roleId") Long roleId) {
    return getProvider().listGroupsByRoleId(roleId);
  }

  public static String listGroupPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listGroupPOsByMetalake(metalakeName);
  }

  public static String listExtendedGroupPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().listExtendedGroupPOsByMetalakeId(metalakeId);
  }

  public static String deleteGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
