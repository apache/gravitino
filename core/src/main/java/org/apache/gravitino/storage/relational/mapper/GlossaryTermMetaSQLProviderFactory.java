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
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects SQL for glossary definitions and synonyms on the configured backend. */
public class GlossaryTermMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, GlossaryTermMetaBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new GlossaryTermMetaMySQLProvider(),
          JDBCBackendType.H2, new GlossaryTermMetaBaseSQLProvider(),
          JDBCBackendType.POSTGRESQL, new GlossaryTermMetaBaseSQLProvider());

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#getForUpdate}. */
  public static String getForUpdate(
      @Param("metalakeId") long metalakeId, @Param("key") String key) {
    return getProvider().getForUpdate(metalakeId, key);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#getByIdForUpdate}. */
  public static String getByIdForUpdate(
      @Param("metalakeId") long metalakeId, @Param("termId") long termId) {
    return getProvider().getByIdForUpdate(metalakeId, termId);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#incrementVersion}. */
  public static String incrementVersion(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("version") long version,
      @Param("auditInfo") String auditInfo) {
    return getProvider().incrementVersion(metalakeId, termId, version, auditInfo);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#getIds}. */
  public static String getIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") String key) {
    return getProvider().getIds(metalakeId, metalakeName, key);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#insert}. */
  public static String insert(@Param("term") GlossaryTermPO term) {
    return getProvider().insert(term);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#update}. */
  public static String update(@Param("term") GlossaryTermPO term, @Param("version") long version) {
    return getProvider().update(term, version);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#synonyms}. */
  public static String synonyms(@Param("termId") long termId) {
    return getProvider().synonyms(termId);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#addSynonym}. */
  public static String addSynonym(
      @Param("termId") long termId,
      @Param("metalakeId") long metalakeId,
      @Param("synonym") String synonym,
      @Param("key") String key) {
    return getProvider().addSynonym(termId, metalakeId, synonym, key);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#clearSynonyms}. */
  public static String clearSynonyms(@Param("termId") long termId) {
    return getProvider().clearSynonyms(termId);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#delete}. */
  public static String delete(
      @Param("termId") long termId,
      @Param("version") long version,
      @Param("deletedAt") long deletedAt) {
    return getProvider().delete(termId, version, deletedAt);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#deleteSynonyms}. */
  public static String deleteSynonyms(
      @Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return getProvider().deleteSynonyms(termId, deletedAt);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#dropMetalake}. */
  public static String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return getProvider().dropMetalake(metalakeId, deletedAt);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#dropMetalakeSynonyms}. */
  public static String dropMetalakeSynonyms(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return getProvider().dropMetalakeSynonyms(metalakeId, deletedAt);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#expired}. */
  public static String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return getProvider().expired(cutoff, limit);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#purgeSynonyms}. */
  public static String purgeSynonyms(@Param("termId") long termId) {
    return getProvider().purgeSynonyms(termId);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#purge}. */
  public static String purge(@Param("termId") long termId) {
    return getProvider().purge(termId);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#snapshot}. */
  public static String snapshot(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") @Nullable String key) {
    return getProvider().snapshot(metalakeId, metalakeName, key);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#selectTermSnapshotsByIds}. */
  public static String selectTermSnapshotsByIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("ids") List<Long> ids) {
    return getProvider().selectTermSnapshotsByIds(metalakeId, metalakeName, ids);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#conflictingName}. */
  public static String conflictingName(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("keys") List<String> keys) {
    return getProvider().conflictingName(metalakeId, excludedTermId, keys);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#conflictingSynonym}. */
  public static String conflictingSynonym(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("key") String key) {
    return getProvider().conflictingSynonym(metalakeId, excludedTermId, key);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#idsByKeys}. */
  public static String idsByKeys(
      @Param("metalakeId") long metalakeId, @Param("keys") List<String> keys) {
    return getProvider().idsByKeys(metalakeId, keys);
  }

  private static GlossaryTermMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  static class GlossaryTermMetaMySQLProvider extends GlossaryTermMetaBaseSQLProvider {
    @Override
    public String getForUpdate(@Param("metalakeId") long metalakeId, @Param("key") String key) {
      // Relation writers lock the primary key before updating the name index. Resolve the ID
      // without locking the name index, then verify the live name on the locked row. MySQL does
      // not extend the outer FOR UPDATE to rows read by a nested subquery, so on 8.0.35
      // performance_schema.data_locks reports only an X,REC_NOT_GAP lock on PRIMARY for this
      // statement, where the plain name predicate also locks the name index record.
      String liveTerm = liveTermByName(metalakeId, key);
      return SELECT_TERM
          + " FORCE INDEX (PRIMARY) WHERE term_id=(SELECT term_id FROM "
          + GlossaryTermMetaMapper.TABLE_NAME
          + " WHERE "
          + liveTerm
          + ") AND "
          + liveTerm
          + " FOR UPDATE";
    }
  }
}
