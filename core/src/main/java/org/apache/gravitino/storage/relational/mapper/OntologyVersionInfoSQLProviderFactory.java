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
import org.apache.gravitino.storage.relational.mapper.provider.base.OntologyVersionInfoBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.OntologyVersionInfoPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.OntologyVersionInfoPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects database-specific SQL providers for ontology version snapshots. */
public class OntologyVersionInfoSQLProviderFactory {

  private static final Map<JDBCBackendType, OntologyVersionInfoBaseSQLProvider>
      ONTOLOGY_VERSION_INFO_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new OntologyVersionInfoMySQLProvider(),
              JDBCBackendType.H2, new OntologyVersionInfoH2Provider(),
              JDBCBackendType.POSTGRESQL, new OntologyVersionInfoPostgreSQLProvider());

  /** Returns the SQL provider for the configured relational backend. */
  public static OntologyVersionInfoBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return ONTOLOGY_VERSION_INFO_SQL_PROVIDER_MAP.get(JDBCBackendType.fromString(databaseId));
  }

  static class OntologyVersionInfoMySQLProvider extends OntologyVersionInfoBaseSQLProvider {}

  static class OntologyVersionInfoH2Provider extends OntologyVersionInfoBaseSQLProvider {}

  /** Provides SQL for inserting an ontology version snapshot. */
  public static String insertOntologyVersionInfo(
      @Param("ontologyVersionInfo") OntologyVersionInfoPO versionInfoPO) {
    return getProvider().insertOntologyVersionInfo(versionInfoPO);
  }

  /** Provides SQL for selecting an ontology version snapshot. */
  public static String selectOntologyVersionInfoByOntologyIdAndVersion(
      @Param("ontologyId") Long ontologyId, @Param("version") Integer version) {
    return getProvider().selectOntologyVersionInfoByOntologyIdAndVersion(ontologyId, version);
  }

  /** Provides SQL for soft-deleting snapshots by ontology ID. */
  public static String softDeleteOntologyVersionsByOntologyId(
      @Param("ontologyId") Long ontologyId) {
    return getProvider().softDeleteOntologyVersionsByOntologyId(ontologyId);
  }

  /** Provides SQL for soft-deleting snapshots by metalake ID. */
  public static String softDeleteOntologyVersionsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteOntologyVersionsByMetalakeId(metalakeId);
  }

  /** Provides SQL for permanently deleting old snapshots. */
  public static String deleteOntologyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteOntologyVersionsByLegacyTimeline(legacyTimeline, limit);
  }

  /** Provides SQL for selecting ontologies that exceed the version retention count. */
  public static String selectOntologyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return getProvider().selectOntologyVersionsByRetentionCount(versionRetentionCount);
  }

  /** Provides SQL for soft-deleting snapshots through a retention line. */
  public static String softDeleteOntologyVersionsByRetentionLine(
      @Param("ontologyId") Long ontologyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return getProvider()
        .softDeleteOntologyVersionsByRetentionLine(ontologyId, versionRetentionLine, limit);
  }
}
