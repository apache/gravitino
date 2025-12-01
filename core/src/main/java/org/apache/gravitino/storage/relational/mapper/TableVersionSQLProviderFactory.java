/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.storage.relational.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.TableVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.TableVersionPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TableVersionSQLProviderFactory {

  private static final Map<JDBCBackendType, TableVersionBaseSQLProvider>
      TABLE_VERSION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new TableVersionSQLProviderFactory.TableVersionMySQLProvider(),
              JDBCBackendType.H2, new TableVersionSQLProviderFactory.TableVersionH2Provider(),
              JDBCBackendType.POSTGRESQL, new TableVersionPostgreSQLProvider());

  public static TableVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return TABLE_VERSION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TableVersionMySQLProvider extends TableVersionBaseSQLProvider {}

  static class TableVersionH2Provider extends TableVersionBaseSQLProvider {}

  public static String insertTableVersion(@Param("tablePO") TablePO tablePO) {
    return getProvider().insertTableVersion(tablePO);
  }

  public static String insertTableVersionOnDuplicateKeyUpdate(@Param("tablePO") TablePO tablePO) {
    return getProvider().insertTableVersionOnDuplicateKeyUpdate(tablePO);
  }

  public static String softDeleteTableVersionByTableIdAndVersion(
      @Param("tableId") Long tableId, @Param("version") Long version) {
    return getProvider().softDeleteTableVersionByTableIdAndVersion(tableId, version);
  }

  public static String deleteTableVersionByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTableVersionByLegacyTimeline(legacyTimeline, limit);
  }
}
