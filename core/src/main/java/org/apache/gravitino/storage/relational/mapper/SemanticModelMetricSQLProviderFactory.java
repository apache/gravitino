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
import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelMetricBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.SemanticModelMetricPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects database-specific SQL providers for Semantic Model metric identities. */
public class SemanticModelMetricSQLProviderFactory {
  private static final Map<JDBCBackendType, SemanticModelMetricBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new SemanticModelMetricBaseSQLProvider(),
          JDBCBackendType.H2, new SemanticModelMetricBaseSQLProvider(),
          JDBCBackendType.POSTGRESQL, new SemanticModelMetricPostgreSQLProvider());

  /** Returns the SQL provider for the configured relational backend. */
  public static SemanticModelMetricBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  /** Inserts a new metric identity. */
  public static String insertSemanticModelMetric(@Param("metric") SemanticModelMetricPO metricPO) {
    return getProvider().insertSemanticModelMetric(metricPO);
  }

  /** Locks and reads the live identities of a Semantic Model. */
  public static String selectSemanticModelMetricsByModelIdForUpdate(
      @Param("semanticModelId") Long semanticModelId) {
    return getProvider().selectSemanticModelMetricsByModelIdForUpdate(semanticModelId);
  }

  /** Retires selected live identities belonging to a Semantic Model. */
  public static String softDeleteSemanticModelMetricsByIds(
      @Param("semanticModelId") Long semanticModelId, @Param("metricIds") List<Long> metricIds) {
    return getProvider().softDeleteSemanticModelMetricsByIds(semanticModelId, metricIds);
  }

  /** Retires all live metric identities of a Semantic Model. */
  public static String softDeleteSemanticModelMetricsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId) {
    return getProvider().softDeleteSemanticModelMetricsBySemanticModelId(semanticModelId);
  }

  /** Retires live metric identities under schemas. */
  public static String softDeleteSemanticModelMetricsBySchemaIds(
      @Param("schemaIds") List<Long> schemaIds) {
    return getProvider().softDeleteSemanticModelMetricsBySchemaIds(schemaIds);
  }

  /** Retires live metric identities under a catalog. */
  public static String softDeleteSemanticModelMetricsByCatalogId(
      @Param("catalogId") Long catalogId) {
    return getProvider().softDeleteSemanticModelMetricsByCatalogId(catalogId);
  }

  /** Retires live metric identities under a metalake. */
  public static String softDeleteSemanticModelMetricsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteSemanticModelMetricsByMetalakeId(metalakeId);
  }

  /** Permanently deletes retired identities older than a timeline. */
  public static String deleteSemanticModelMetricsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteSemanticModelMetricsByLegacyTimeline(legacyTimeline, limit);
  }
}
