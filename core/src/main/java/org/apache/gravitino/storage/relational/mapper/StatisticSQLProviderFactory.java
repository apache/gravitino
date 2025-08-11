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
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.provider.base.StatisticBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.StatisticPostgresSQLProvider;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class StatisticSQLProviderFactory {

  static class StatisticMySQLProvider extends StatisticBaseSQLProvider {}

  static class StatisticH2Provider extends StatisticBaseSQLProvider {}

  private static final Map<JDBCBackend.JDBCBackendType, StatisticBaseSQLProvider>
      STATISTIC_SQL_PROVIDERS =
          ImmutableMap.of(
              JDBCBackend.JDBCBackendType.H2,
              new StatisticH2Provider(),
              JDBCBackend.JDBCBackendType.MYSQL,
              new StatisticMySQLProvider(),
              JDBCBackend.JDBCBackendType.POSTGRESQL,
              new StatisticPostgresSQLProvider());

  public static StatisticBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    JDBCBackend.JDBCBackendType jdbcBackendType =
        JDBCBackend.JDBCBackendType.fromString(databaseId);
    return STATISTIC_SQL_PROVIDERS.get(jdbcBackendType);
  }

  public static String batchInsertStatisticPOsOnDuplicateKeyUpdate(
      @Param("statisticPOs") List<StatisticPO> statisticPOs) {
    return getProvider().batchInsertStatisticPOsOnDuplicateKeyUpdate(statisticPOs);
  }

  public static String batchDeleteStatisticPOs(
      @Param("entityId") Long entityId, @Param("statisticNames") List<String> statisticNames) {
    return getProvider().batchDeleteStatisticPOs(entityId, statisticNames);
  }

  public static String softDeleteStatisticsByEntityId(@Param("entityId") Long entityId) {
    return getProvider().softDeleteStatisticsByEntityId(entityId);
  }

  public static String listStatisticPOsByEntityId(
      @Param("metalakeId") Long metalakeId, @Param("entityId") Long entityId) {
    return getProvider().listStatisticPOsByEntityId(metalakeId, entityId);
  }

  public static String softDeleteStatisticsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteStatisticsByMetalakeId(metalakeId);
  }

  public static String softDeleteStatisticsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteStatisticsByCatalogId(catalogId);
  }

  public static String softDeleteStatisticsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteStatisticsBySchemaId(schemaId);
  }

  public static String deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteStatisticsByLegacyTimeline(legacyTimeline, limit);
  }
}
