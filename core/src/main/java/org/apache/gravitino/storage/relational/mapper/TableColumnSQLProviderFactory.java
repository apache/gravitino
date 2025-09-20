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
import org.apache.gravitino.storage.relational.mapper.provider.base.TableColumnBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.TableColumnPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TableColumnSQLProviderFactory {

  static class TableColumnH2Provider extends TableColumnBaseSQLProvider {}

  static class TableColumnMySQLProvider extends TableColumnBaseSQLProvider {}

  private static final Map<JDBCBackend.JDBCBackendType, TableColumnBaseSQLProvider>
      TABLE_COLUMN_SQL_PROVIDERS =
          ImmutableMap.of(
              JDBCBackend.JDBCBackendType.MYSQL, new TableColumnMySQLProvider(),
              JDBCBackend.JDBCBackendType.H2, new TableColumnH2Provider(),
              JDBCBackend.JDBCBackendType.POSTGRESQL, new TableColumnPostgreSQLProvider());

  public static TableColumnBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    JDBCBackend.JDBCBackendType jdbcBackendType =
        JDBCBackend.JDBCBackendType.fromString(databaseId);
    return TABLE_COLUMN_SQL_PROVIDERS.get(jdbcBackendType);
  }

  public static String listColumnPOsByTableIdAndVersion(
      @Param("tableId") Long tableId, @Param("tableVersion") Long tableVersion) {
    return getProvider().listColumnPOsByTableIdAndVersion(tableId, tableVersion);
  }

  public static String insertColumnPOs(@Param("columnPOs") List<ColumnPO> columnPOs) {
    return getProvider().insertColumnPOs(columnPOs);
  }

  public static String updateSchemaIdByTableId(
      @Param("tableId") Long tableId, @Param("newSchemaId") Long newSchemaId) {
    return getProvider().updateSchemaIdByTableId(tableId, newSchemaId);
  }

  public static String softDeleteColumnsByTableId(@Param("tableId") Long tableId) {
    return getProvider().softDeleteColumnsByTableId(tableId);
  }

  public static String deleteColumnPOsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteColumnPOsByLegacyTimeline(legacyTimeline, limit);
  }

  public static String softDeleteColumnsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteColumnsByMetalakeId(metalakeId);
  }

  public static String softDeleteColumnsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteColumnsByCatalogId(catalogId);
  }

  public static String softDeleteColumnsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteColumnsBySchemaId(schemaId);
  }

  public static String selectColumnIdByTableIdAndName(
      @Param("tableId") Long tableId, @Param("columnName") String name) {
    return getProvider().selectColumnIdByTableIdAndName(tableId, name);
  }

  public static String selectColumnPOById(@Param("columnId") Long columnId) {
    return getProvider().selectColumnPOById(columnId);
  }

  public static String listColumnPOsByColumnIds(@Param("columnIds") List<Long> columnIds) {
    return getProvider().listColumnPOsByColumnIds(columnIds);
  }
}
