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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserGroupRelBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpUserGroupRelH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpUserGroupRelPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.annotations.Param;

public class IdpUserGroupRelSQLProviderFactory {
  private static final IdpUserGroupRelBaseSQLProvider MYSQL_PROVIDER =
      new IdpUserGroupRelBaseSQLProvider();
  private static final IdpUserGroupRelBaseSQLProvider H2_PROVIDER = new IdpUserGroupRelH2Provider();
  private static final IdpUserGroupRelBaseSQLProvider POSTGRESQL_PROVIDER =
      new IdpUserGroupRelPostgreSQLProvider();
  private static final Map<JDBCBackendType, IdpUserGroupRelBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private IdpUserGroupRelSQLProviderFactory() {}

  public static String selectGroupNamesByUsername(@Param("username") String username) {
    return currentProvider().selectGroupNamesByUsername(username);
  }

  public static String selectUsernamesByGroupName(@Param("groupName") String groupName) {
    return currentProvider().selectUsernamesByGroupName(groupName);
  }

  public static String batchInsertRelations(@Param("relations") List<IdpUserGroupRelPO> relations) {
    return currentProvider().batchInsertRelations(relations);
  }

  public static String softDeleteRelations(
      @Param("groupName") String groupName, @Param("usernames") List<String> usernames) {
    return currentProvider().softDeleteRelations(groupName, usernames);
  }

  public static String softDeleteRelationsByUsername(@Param("username") String username) {
    return currentProvider().softDeleteRelationsByUsername(username);
  }

  public static String softDeleteRelationsByGroupName(@Param("groupName") String groupName) {
    return currentProvider().softDeleteRelationsByGroupName(groupName);
  }

  public static String deleteIdpUserGroupRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpUserGroupRelMetasByLegacyTimeline(legacyTimeline, limit);
  }

  static IdpUserGroupRelBaseSQLProvider getProvider(String databaseId) {
    return SQLProviderFactoryHelper.getProvider(
        databaseId, PROVIDER_MAP, IdpUserGroupRelSQLProviderFactory.class);
  }

  private static IdpUserGroupRelBaseSQLProvider currentProvider() {
    return SQLProviderFactoryHelper.currentProvider(
        PROVIDER_MAP, IdpUserGroupRelSQLProviderFactory.class);
  }
}
