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
import java.util.Map;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaSQLProviderFactory {
  private static final IdpGroupMetaBaseSQLProvider MYSQL_PROVIDER =
      new IdpGroupMetaBaseSQLProvider();
  private static final IdpGroupMetaBaseSQLProvider H2_PROVIDER = new IdpGroupMetaH2Provider();
  private static final IdpGroupMetaBaseSQLProvider POSTGRESQL_PROVIDER =
      new IdpGroupMetaPostgreSQLProvider();
  private static final Map<JDBCBackendType, IdpGroupMetaBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private IdpGroupMetaSQLProviderFactory() {}

  public static String selectIdpGroup(@Param("groupName") String groupName) {
    return currentProvider().selectIdpGroup(groupName);
  }

  public static String selectIdpGroupWithUsers(@Param("groupName") String groupName) {
    return currentProvider().selectIdpGroupWithUsers(groupName);
  }

  public static String insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return currentProvider().insertIdpGroup(groupPO);
  }

  public static String softDeleteIdpGroup(@Param("groupName") String groupName) {
    return currentProvider().softDeleteIdpGroup(groupName);
  }

  public static String deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }

  static IdpGroupMetaBaseSQLProvider getProvider(String databaseId) {
    return SQLProviderFactoryHelper.getProvider(
        databaseId, PROVIDER_MAP, IdpGroupMetaSQLProviderFactory.class);
  }

  private static IdpGroupMetaBaseSQLProvider currentProvider() {
    return SQLProviderFactoryHelper.currentProvider(
        PROVIDER_MAP, IdpGroupMetaSQLProviderFactory.class);
  }
}
