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
import org.apache.gravitino.storage.relational.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.h2.IdpGroupMetaH2Provider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.IdpGroupMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, IdpGroupMetaBaseSQLProvider>
      IDP_GROUP_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new IdpGroupMetaMySQLProvider(),
              JDBCBackendType.H2, new IdpGroupMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new IdpGroupMetaPostgreSQLProvider());

  public static IdpGroupMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return IDP_GROUP_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class IdpGroupMetaMySQLProvider extends IdpGroupMetaBaseSQLProvider {}

  public static String selectLocalGroup(@Param("groupName") String groupName) {
    return getProvider().selectLocalGroup(groupName);
  }

  public static String insertLocalGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return getProvider().insertLocalGroup(groupPO);
  }

  public static String softDeleteLocalGroup(
      @Param("groupId") Long groupId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return getProvider().softDeleteLocalGroup(groupId, deletedAt, auditInfo);
  }

  public static String truncateLocalGroupMeta() {
    return getProvider().truncateLocalGroupMeta();
  }
}
