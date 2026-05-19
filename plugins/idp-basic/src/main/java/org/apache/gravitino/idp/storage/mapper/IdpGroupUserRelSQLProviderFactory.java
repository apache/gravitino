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

package org.apache.gravitino.idp.storage.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupUserRelBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupUserRelH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupUserRelPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.annotations.Param;

public class IdpGroupUserRelSQLProviderFactory {
  private static final IdpGroupUserRelBaseSQLProvider MYSQL_PROVIDER =
      new IdpGroupUserRelBaseSQLProvider();
  private static final IdpGroupUserRelBaseSQLProvider H2_PROVIDER = new IdpGroupUserRelH2Provider();
  private static final IdpGroupUserRelBaseSQLProvider POSTGRESQL_PROVIDER =
      new IdpGroupUserRelPostgreSQLProvider();
  private static final Map<JDBCBackendType, IdpGroupUserRelBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private IdpGroupUserRelSQLProviderFactory() {}

  private static IdpGroupUserRelBaseSQLProvider currentProvider() {
    return SQLProviderFactoryHelper.currentProvider(
        PROVIDER_MAP, IdpGroupUserRelSQLProviderFactory.class);
  }

  static IdpGroupUserRelBaseSQLProvider getProvider(String databaseId) {
    return SQLProviderFactoryHelper.getProvider(
        databaseId, PROVIDER_MAP, IdpGroupUserRelSQLProviderFactory.class);
  }

  public static String selectGroupNamesByUserId(@Param("userId") Long userId) {
    return currentProvider().selectGroupNamesByUserId(userId);
  }

  public static String selectUsernamesByGroupId(@Param("groupId") Long groupId) {
    return currentProvider().selectUsernamesByGroupId(groupId);
  }

  public static String batchInsertRelations(@Param("relations") List<IdpGroupUserRelPO> relations) {
    return currentProvider().batchInsertRelations(relations);
  }

  public static String softDeleteRelations(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return currentProvider().softDeleteRelations(groupId, userIds);
  }

  public static String softDeleteRelationsByUsername(@Param("username") String username) {
    return currentProvider().softDeleteRelationsByUsername(username);
  }

  public static String softDeleteRelationsByGroupName(@Param("groupName") String groupName) {
    return currentProvider().softDeleteRelationsByGroupName(groupName);
  }

  public static String deleteIdpGroupUserRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpGroupUserRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
