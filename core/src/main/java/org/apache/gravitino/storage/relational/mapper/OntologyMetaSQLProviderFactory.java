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
import org.apache.gravitino.storage.relational.mapper.provider.base.OntologyMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.OntologyMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.OntologyPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects database-specific SQL providers for ontology identity metadata. */
public class OntologyMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, OntologyMetaBaseSQLProvider>
      ONTOLOGY_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new OntologyMetaMySQLProvider(),
              JDBCBackendType.H2, new OntologyMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new OntologyMetaPostgreSQLProvider());

  /** Returns the SQL provider for the configured relational backend. */
  public static OntologyMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return ONTOLOGY_META_SQL_PROVIDER_MAP.get(JDBCBackendType.fromString(databaseId));
  }

  static class OntologyMetaMySQLProvider extends OntologyMetaBaseSQLProvider {}

  static class OntologyMetaH2Provider extends OntologyMetaBaseSQLProvider {}

  /** Provides SQL for listing ontologies by metalake ID. */
  public static String listOntologyPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().listOntologyPOsByMetalakeId(metalakeId);
  }

  /** Provides SQL for listing ontologies by metalake name. */
  public static String listOntologyPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName) {
    return getProvider().listOntologyPOsByFullQualifiedName(metalakeName);
  }

  /** Provides SQL for selecting an ontology ID by metalake ID and name. */
  public static String selectOntologyIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return getProvider().selectOntologyIdByMetalakeIdAndName(metalakeId, ontologyName);
  }

  /** Provides SQL for selecting an ontology by metalake ID and name. */
  public static String selectOntologyMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return getProvider().selectOntologyMetaByMetalakeIdAndName(metalakeId, ontologyName);
  }

  /** Provides SQL for selecting and locking an ontology identity by natural key. */
  public static String selectOntologyMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return getProvider().selectOntologyMetaByMetalakeIdAndNameForUpdate(metalakeId, ontologyName);
  }

  /** Provides SQL for selecting an ontology by stable ID. */
  public static String selectOntologyMetaById(@Param("ontologyId") Long ontologyId) {
    return getProvider().selectOntologyMetaById(ontologyId);
  }

  /** Provides SQL for selecting and locking an ontology identity by stable ID. */
  public static String selectOntologyMetaByIdForUpdate(@Param("ontologyId") Long ontologyId) {
    return getProvider().selectOntologyMetaByIdForUpdate(ontologyId);
  }

  /** Provides SQL for selecting an ontology by metalake and ontology name. */
  public static String selectOntologyByFullQualifiedName(
      @Param("metalakeName") String metalakeName, @Param("ontologyName") String ontologyName) {
    return getProvider().selectOntologyByFullQualifiedName(metalakeName, ontologyName);
  }

  /** Provides SQL for inserting an ontology identity. */
  public static String insertOntologyMeta(@Param("ontologyMeta") OntologyPO ontologyPO) {
    return getProvider().insertOntologyMeta(ontologyPO);
  }

  /** Provides SQL for updating an ontology identity with a new version. */
  public static String updateOntologyMeta(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO) {
    return getProvider().updateOntologyMeta(newOntologyPO, oldOntologyPO);
  }

  /** Provides SQL for updating only the ontology status. */
  public static String updateOntologyMetaStatus(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO) {
    return getProvider().updateOntologyMetaStatus(newOntologyPO, oldOntologyPO);
  }

  /** Provides SQL for soft-deleting an ontology identity by stable ID and current version. */
  public static String softDeleteOntologyMetasByOntologyId(
      @Param("ontologyId") Long ontologyId, @Param("currentVersion") Integer currentVersion) {
    return getProvider().softDeleteOntologyMetasByOntologyId(ontologyId, currentVersion);
  }

  /** Provides SQL for soft-deleting ontology identities by metalake ID. */
  public static String softDeleteOntologyMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteOntologyMetasByMetalakeId(metalakeId);
  }

  /** Provides SQL for permanently deleting old ontology identities. */
  public static String deleteOntologyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteOntologyMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
