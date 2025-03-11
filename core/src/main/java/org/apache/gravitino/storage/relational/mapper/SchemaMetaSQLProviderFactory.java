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
import org.apache.gravitino.storage.relational.mapper.provider.base.SchemaMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.SchemaMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, SchemaMetaBaseSQLProvider>
      SCHEMA_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new SchemaMetaMySQLProvider(),
              JDBCBackendType.H2, new SchemaMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new SchemaMetaPostgreSQLProvider());

  public static SchemaMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return SCHEMA_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class SchemaMetaMySQLProvider extends SchemaMetaBaseSQLProvider {}

  static class SchemaMetaH2Provider extends SchemaMetaBaseSQLProvider {}

  public static String listSchemaPOsBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return getProvider().listSchemaPOsBySchemaIds(schemaIds);
  }

  public static String listSchemaPOsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().listSchemaPOsByCatalogId(catalogId);
  }

  public static String selectSchemaIdByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return getProvider().selectSchemaIdByCatalogIdAndName(catalogId, name);
  }

  public static String selectSchemaMetaByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return getProvider().selectSchemaMetaByCatalogIdAndName(catalogId, name);
  }

  public static String selectSchemaMetaById(@Param("schemaId") Long schemaId) {
    return getProvider().selectSchemaMetaById(schemaId);
  }

  public static String insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO) {
    return getProvider().insertSchemaMeta(schemaPO);
  }

  public static String insertSchemaMetaOnDuplicateKeyUpdate(
      @Param("schemaMeta") SchemaPO schemaPO) {
    return getProvider().insertSchemaMetaOnDuplicateKeyUpdate(schemaPO);
  }

  public static String updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO) {
    return getProvider().updateSchemaMeta(newSchemaPO, oldSchemaPO);
  }

  public static String softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteSchemaMetasBySchemaId(schemaId);
  }

  public static String softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteSchemaMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteSchemaMetasByCatalogId(catalogId);
  }

  public static String deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteSchemaMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
