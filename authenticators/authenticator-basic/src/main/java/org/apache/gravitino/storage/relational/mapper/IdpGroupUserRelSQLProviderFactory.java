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
import org.apache.gravitino.storage.relational.mapper.provider.base.IdpGroupUserRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.IdpGroupUserRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class IdpGroupUserRelSQLProviderFactory {

  private static final Map<JDBCBackendType, IdpGroupUserRelBaseSQLProvider>
      IDP_GROUP_USER_RELATION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new IdpGroupUserRelMySQLProvider(),
              JDBCBackendType.H2, new IdpGroupUserRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new IdpGroupUserRelPostgreSQLProvider());

  public static IdpGroupUserRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return IDP_GROUP_USER_RELATION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class IdpGroupUserRelMySQLProvider extends IdpGroupUserRelBaseSQLProvider {}

  static class IdpGroupUserRelH2Provider extends IdpGroupUserRelBaseSQLProvider {}

  public static String selectGroupNamesByUserId(@Param("userId") Long userId) {
    return getProvider().selectGroupNamesByUserId(userId);
  }

  public static String selectUserNamesByGroupId(@Param("groupId") Long groupId) {
    return getProvider().selectUserNamesByGroupId(groupId);
  }

  public static String selectRelatedUserIds(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return getProvider().selectRelatedUserIds(groupId, userIds);
  }

  public static String batchInsertIdpGroupUsers(
      @Param("relations") List<IdpGroupUserRelPO> relations) {
    return getProvider().batchInsertIdpGroupUsers(relations);
  }

  public static String softDeleteIdpGroupUsers(
      @Param("groupId") Long groupId,
      @Param("userIds") List<Long> userIds,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return getProvider().softDeleteIdpGroupUsers(groupId, userIds, deletedAt, auditInfo);
  }

  public static String softDeleteGroupUsersByUserId(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return getProvider().softDeleteGroupUsersByUserId(userId, deletedAt, auditInfo);
  }

  public static String softDeleteGroupUsersByGroupId(
      @Param("groupId") Long groupId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return getProvider().softDeleteGroupUsersByGroupId(groupId, deletedAt, auditInfo);
  }

  public static String deleteIdpGroupUserRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteIdpGroupUserRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
