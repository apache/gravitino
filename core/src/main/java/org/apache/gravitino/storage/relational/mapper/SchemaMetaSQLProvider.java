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
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class SchemaMetaSQLProvider {
  private static final Map<JDBCBackendType, SchemaMetaBaseProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new SchemaMetaMySQLProvider(),
          JDBCBackendType.H2, new SchemaMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new SchemaMetaPostgreSQLProvider());

  public static SchemaMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class SchemaMetaMySQLProvider extends SchemaMetaBaseProvider {}

  static class SchemaMetaH2Provider extends SchemaMetaBaseProvider {}

  static class SchemaMetaPostgreSQLProvider extends SchemaMetaBaseProvider {}

  public String listSchemaPOsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().listSchemaPOsByCatalogId(catalogId);
  }

  public String selectSchemaIdByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return getProvider().selectSchemaIdByCatalogIdAndName(catalogId, name);
  }

  public String selectSchemaMetaByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name) {
    return getProvider().selectSchemaMetaByCatalogIdAndName(catalogId, name);
  }

  public String selectSchemaMetaById(@Param("schemaId") Long schemaId) {
    return getProvider().selectSchemaMetaById(schemaId);
  }

  public String insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO) {
    return getProvider().insertSchemaMeta(schemaPO);
  }

  public String insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO) {
    return getProvider().insertSchemaMetaOnDuplicateKeyUpdate(schemaPO);
  }

  public String updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO) {
    return getProvider().updateSchemaMeta(newSchemaPO, oldSchemaPO);
  }

  public String softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteSchemaMetasBySchemaId(schemaId);
  }

  public String softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteSchemaMetasByMetalakeId(metalakeId);
  }

  public String softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteSchemaMetasByCatalogId(catalogId);
  }

  public String deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteSchemaMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
