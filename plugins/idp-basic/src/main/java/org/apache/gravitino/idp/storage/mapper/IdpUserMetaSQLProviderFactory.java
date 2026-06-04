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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpUserMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpUserMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaSQLProviderFactory {
  private static final IdpUserMetaBaseSQLProvider MYSQL_PROVIDER = new IdpUserMetaBaseSQLProvider();
  private static final IdpUserMetaBaseSQLProvider H2_PROVIDER = new IdpUserMetaH2Provider();
  private static final IdpUserMetaBaseSQLProvider POSTGRESQL_PROVIDER =
      new IdpUserMetaPostgreSQLProvider();
  private static final Map<JDBCBackendType, IdpUserMetaBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL,
          MYSQL_PROVIDER,
          JDBCBackendType.H2,
          H2_PROVIDER,
          JDBCBackendType.POSTGRESQL,
          POSTGRESQL_PROVIDER);

  private IdpUserMetaSQLProviderFactory() {}

  public static String selectIdpUser(@Param("username") String username) {
    return currentProvider().selectIdpUser(username);
  }

  public static String selectIdpUserWithGroups(@Param("username") String username) {
    return currentProvider().selectIdpUserWithGroups(username);
  }

  public static String selectIdpUsersByUsernames(@Param("usernames") List<String> usernames) {
    return currentProvider().selectIdpUsersByUsernames(usernames);
  }

  public static String insertIdpUser(@Param("userMeta") IdpUserPO userPO) {
    return currentProvider().insertIdpUser(userPO);
  }

  public static String updateIdpUserPassword(
      @Param("username") String username, @Param("passwordHash") String passwordHash) {
    return currentProvider().updateIdpUserPassword(username, passwordHash);
  }

  public static String softDeleteIdpUser(@Param("username") String username) {
    return currentProvider().softDeleteIdpUser(username);
  }

  public static String deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpUserMetasByLegacyTimeline(legacyTimeline, limit);
  }

  static IdpUserMetaBaseSQLProvider getProvider(String databaseId) {
    return SQLProviderFactoryHelper.getProvider(
        databaseId, PROVIDER_MAP, IdpUserMetaSQLProviderFactory.class);
  }

  private static IdpUserMetaBaseSQLProvider currentProvider() {
    return SQLProviderFactoryHelper.currentProvider(
        PROVIDER_MAP, IdpUserMetaSQLProviderFactory.class);
  }
}
