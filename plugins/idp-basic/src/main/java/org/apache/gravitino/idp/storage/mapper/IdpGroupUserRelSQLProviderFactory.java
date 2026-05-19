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

import java.util.List;
import org.apache.gravitino.idp.storage.mapper.SQLProviderFactoryHelper.ProviderMap;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupUserRelBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupUserRelH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupUserRelPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupUserRelPO;
import org.apache.ibatis.annotations.Param;

public class IdpGroupUserRelSQLProviderFactory {
  private static final ProviderMap<IdpGroupUserRelBaseSQLProvider> PROVIDER_MAP =
      SQLProviderFactoryHelper.providerMap(
          IdpGroupUserRelSQLProviderFactory.class,
          new IdpGroupUserRelBaseSQLProvider(),
          new IdpGroupUserRelH2Provider(),
          new IdpGroupUserRelPostgreSQLProvider());

  private IdpGroupUserRelSQLProviderFactory() {}

  public static String selectGroupNamesByUserId(@Param("userId") Long userId) {
    return PROVIDER_MAP.currentProvider().selectGroupNamesByUserId(userId);
  }

  public static String selectUserNamesByGroupId(@Param("groupId") Long groupId) {
    return PROVIDER_MAP.currentProvider().selectUserNamesByGroupId(groupId);
  }

  public static String selectRelatedUserIds(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return PROVIDER_MAP.currentProvider().selectRelatedUserIds(groupId, userIds);
  }

  public static String batchInsertIdpGroupUsers(
      @Param("relations") List<IdpGroupUserRelPO> relations) {
    return PROVIDER_MAP.currentProvider().batchInsertIdpGroupUsers(relations);
  }

  public static String softDeleteIdpGroupUsers(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return PROVIDER_MAP.currentProvider().softDeleteIdpGroupUsers(groupId, userIds);
  }

  public static String softDeleteGroupUsersByUserId(@Param("userId") Long userId) {
    return PROVIDER_MAP.currentProvider().softDeleteGroupUsersByUserId(userId);
  }

  public static String softDeleteGroupUsersByGroupId(@Param("groupId") Long groupId) {
    return PROVIDER_MAP.currentProvider().softDeleteGroupUsersByGroupId(groupId);
  }

  public static String deleteIdpGroupUserRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return PROVIDER_MAP
        .currentProvider()
        .deleteIdpGroupUserRelMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
