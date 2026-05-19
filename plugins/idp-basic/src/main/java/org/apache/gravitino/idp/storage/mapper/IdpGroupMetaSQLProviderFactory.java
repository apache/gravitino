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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpGroupMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpGroupMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaSQLProviderFactory {
  private static final ProviderMap<IdpGroupMetaBaseSQLProvider> PROVIDER_MAP =
      SQLProviderFactoryHelper.providerMap(
          IdpGroupMetaSQLProviderFactory.class,
          new IdpGroupMetaBaseSQLProvider(),
          new IdpGroupMetaH2Provider(),
          new IdpGroupMetaPostgreSQLProvider());

  private IdpGroupMetaSQLProviderFactory() {}

  private static IdpGroupMetaBaseSQLProvider currentProvider() {
    return PROVIDER_MAP.currentProvider();
  }

  public static String selectIdpGroup(@Param("groupName") String groupName) {
    return currentProvider().selectIdpGroup(groupName);
  }

  public static String selectIdpGroups(@Param("groupNames") List<String> groupNames) {
    return currentProvider().selectIdpGroups(groupNames);
  }

  public static String insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return currentProvider().insertIdpGroup(groupPO);
  }

  public static String softDeleteIdpGroup(@Param("groupId") Long groupId) {
    return currentProvider().softDeleteIdpGroup(groupId);
  }

  public static String deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpGroupMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
