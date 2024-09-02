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
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TableMetaSQLProvider {

  private static final Map<JDBCBackendType, TableMetaBaseProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TableMetaMySQLProvider(),
          JDBCBackendType.H2, new TableMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new TableMetaPostgreSQLProvider());

  public static TableMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TableMetaMySQLProvider extends TableMetaBaseProvider {}

  static class TableMetaH2Provider extends TableMetaBaseProvider {}

  static class TableMetaPostgreSQLProvider extends TableMetaBaseProvider {}

  public String listTablePOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listTablePOsBySchemaId(schemaId);
  }

  public String selectTableIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return getProvider().selectTableIdBySchemaIdAndName(schemaId, name);
  }

  public String selectTableMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return getProvider().selectTableMetaBySchemaIdAndName(schemaId, name);
  }

  public String selectTableMetaById(@Param("tableId") Long tableId) {
    return getProvider().selectTableMetaById(tableId);
  }

  public String insertTableMeta(@Param("tableMeta") TablePO tablePO) {
    return getProvider().insertTableMeta(tablePO);
  }

  public String insertTableMetaOnDuplicateKeyUpdate(@Param("tableMeta") TablePO tablePO) {
    return getProvider().insertTableMetaOnDuplicateKeyUpdate(tablePO);
  }

  public String updateTableMeta(
      @Param("newTableMeta") TablePO newTablePO, @Param("oldTableMeta") TablePO oldTablePO) {
    return getProvider().updateTableMeta(newTablePO, oldTablePO);
  }

  public String softDeleteTableMetasByTableId(@Param("tableId") Long tableId) {
    return getProvider().softDeleteTableMetasByTableId(tableId);
  }

  public String softDeleteTableMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTableMetasByMetalakeId(metalakeId);
  }

  public String softDeleteTableMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteTableMetasByCatalogId(catalogId);
  }

  public String softDeleteTableMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteTableMetasBySchemaId(schemaId);
  }

  public String deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTableMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
