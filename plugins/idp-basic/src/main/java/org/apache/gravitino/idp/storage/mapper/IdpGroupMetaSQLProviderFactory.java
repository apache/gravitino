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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaSQLProviderFactory
    extends IdpBaseSQLProviderFactory<IdpGroupMetaBaseSQLProvider> {
  private static final IdpGroupMetaSQLProviderFactory INSTANCE =
      new IdpGroupMetaSQLProviderFactory();

  private IdpGroupMetaSQLProviderFactory() {
    super(
        "IdP group SQL provider",
        new IdpGroupMetaBaseSQLProvider(),
        new IdpGroupMetaH2Provider(),
        new IdpGroupMetaPostgreSQLProvider());
  }

  public static IdpGroupMetaBaseSQLProvider h2Provider() {
    return INSTANCE.h2ProviderInstance();
  }

  public static IdpGroupMetaBaseSQLProvider mysqlProvider() {
    return INSTANCE.mysqlProviderInstance();
  }

  public static IdpGroupMetaBaseSQLProvider postgresqlProvider() {
    return INSTANCE.postgresqlProviderInstance();
  }

  static IdpGroupMetaBaseSQLProvider getProvider(String databaseId) {
    return INSTANCE.resolveProvider(databaseId);
  }

  public static String selectIdpGroup(@Param("groupName") String groupName) {
    return INSTANCE.currentProviderInstance().selectIdpGroup(groupName);
  }

  public static String selectIdpGroups(@Param("groupNames") List<String> groupNames) {
    return INSTANCE.currentProviderInstance().selectIdpGroups(groupNames);
  }

  public static String insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return INSTANCE.currentProviderInstance().insertIdpGroup(groupPO);
  }

  public static String softDeleteIdpGroup(@Param("groupId") Long groupId) {
    return INSTANCE.currentProviderInstance().softDeleteIdpGroup(groupId);
  }

  public static String deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return INSTANCE
        .currentProviderInstance()
        .deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
