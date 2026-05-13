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
import org.apache.gravitino.storage.relational.mapper.provider.base.LanceTableVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.LanceTableVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class LanceTableVersionSQLProviderFactory {
  private static final Map<JDBCBackendType, LanceTableVersionBaseSQLProvider> SQL_PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new LanceTableVersionProvider(),
          JDBCBackendType.H2, new LanceTableVersionProvider(),
          JDBCBackendType.POSTGRESQL, new LanceTableVersionProvider());

  private LanceTableVersionSQLProviderFactory() {}

  public static LanceTableVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return SQL_PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  static class LanceTableVersionProvider extends LanceTableVersionBaseSQLProvider {}

  public static String insertTableVersion(@Param("tableVersion") LanceTableVersionPO tableVersion) {
    return getProvider().insertTableVersion(tableVersion);
  }

  public static String batchInsertTableVersions(
      @Param("tableVersions") List<LanceTableVersionPO> tableVersions) {
    return getProvider().batchInsertTableVersions(tableVersions);
  }

  public static String selectTableVersion(
      @Param("tableId") Long tableId, @Param("version") Long version) {
    return getProvider().selectTableVersion(tableId, version);
  }

  public static String listTableVersions(
      @Param("tableId") Long tableId,
      @Param("limit") Integer limit,
      @Param("startVersionExclusive") Long startVersionExclusive,
      @Param("descending") boolean descending) {
    return getProvider().listTableVersions(tableId, limit, startVersionExclusive, descending);
  }

  public static String softDeleteTableVersions(
      @Param("tableId") Long tableId,
      @Param("versions") List<Long> versions,
      @Param("deletedAt") Long deletedAt) {
    return getProvider().softDeleteTableVersions(tableId, versions, deletedAt);
  }
}
