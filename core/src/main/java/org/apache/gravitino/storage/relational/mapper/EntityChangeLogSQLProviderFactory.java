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

import static org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper.ENTITY_CHANGE_LOG_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.EntityChangeLogBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.auth.OperateType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class EntityChangeLogSQLProviderFactory {

  private static final Map<JDBCBackendType, EntityChangeLogBaseSQLProvider>
      ENTITY_CHANGE_LOG_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new EntityChangeLogMySQLProvider(),
              JDBCBackendType.H2, new EntityChangeLogH2Provider(),
              JDBCBackendType.POSTGRESQL, new EntityChangeLogPostgreSQLProvider());

  public static EntityChangeLogBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return ENTITY_CHANGE_LOG_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class EntityChangeLogMySQLProvider extends EntityChangeLogBaseSQLProvider {}

  static class EntityChangeLogH2Provider extends EntityChangeLogBaseSQLProvider {}

  static class EntityChangeLogPostgreSQLProvider extends EntityChangeLogBaseSQLProvider {
    @Override
    public String pruneOldEntityChanges(@Param("before") long before) {
      return "DELETE FROM "
          + ENTITY_CHANGE_LOG_TABLE_NAME
          + " WHERE id IN (SELECT id FROM "
          + ENTITY_CHANGE_LOG_TABLE_NAME
          + " WHERE created_at < #{before} LIMIT 1000)";
    }
  }

  public static String selectEntityChanges(
      @Param("createdAtAfter") long createdAtAfter, @Param("maxRows") int maxRows) {
    return getProvider().selectEntityChanges(createdAtAfter, maxRows);
  }

  public static String insertEntityChange(
      @Param("metalakeName") String metalakeName,
      @Param("entityType") String entityType,
      @Param("fullName") String fullName,
      @Param("operateType") OperateType operateType,
      @Param("createdAt") long createdAt) {
    return getProvider()
        .insertEntityChange(metalakeName, entityType, fullName, operateType, createdAt);
  }

  public static String pruneOldEntityChanges(@Param("before") long before) {
    return getProvider().pruneOldEntityChanges(before);
  }
}
