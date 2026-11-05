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
import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelSourceBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.SemanticModelSourcePostgreSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects database-specific SQL for retiring and purging dataset source associations. */
public class SemanticModelSourceSQLProviderFactory {
  private static final Map<JDBCBackendType, SemanticModelSourceBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new SemanticModelSourceBaseSQLProvider(),
          JDBCBackendType.H2, new SemanticModelSourceBaseSQLProvider(),
          JDBCBackendType.POSTGRESQL, new SemanticModelSourcePostgreSQLProvider());

  /** Returns the SQL provider for the configured relational backend. */
  public static SemanticModelSourceBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  /** Retires all live associations of a model. */
  public static String softDeleteByModelId(@Param("modelId") Long modelId) {
    return getProvider().softDeleteByModelId(modelId);
  }

  /** Retires live associations owned by models in the given schemas. */
  public static String softDeleteBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return getProvider().softDeleteBySchemaIds(schemaIds);
  }

  /** Retires live associations owned by models in a catalog. */
  public static String softDeleteByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteByCatalogId(catalogId);
  }

  /** Retires live associations owned by models in a metalake. */
  public static String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteByMetalakeId(metalakeId);
  }

  /** Permanently deletes retired associations older than a timeline. */
  public static String deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteByLegacyTimeline(legacyTimeline, limit);
  }
}
