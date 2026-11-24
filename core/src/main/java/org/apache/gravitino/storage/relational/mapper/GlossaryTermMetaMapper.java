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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermSnapshotPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** Glossary definition and synonym persistence in the caller transaction. */
public interface GlossaryTermMetaMapper {
  /** Table containing glossary definitions. */
  String TABLE_NAME = "glossary_term_meta";

  /** Table containing glossary term synonyms. */
  String SYNONYM_TABLE_NAME = "glossary_term_synonym";

  /** Looks up a live conflict on another term by normalized key. */
  @Nullable
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "conflictingName")
  Long conflictingName(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("keys") List<String> keys);

  /** Looks up a live conflict on another term by normalized key. */
  @Nullable
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "conflictingSynonym")
  Long conflictingSynonym(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("key") String key);

  /** Locks the live definition, or returns null; the caller must fence its metalake first. */
  @Nullable
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "getForUpdate")
  GlossaryTermPO getForUpdate(@Param("metalakeId") long metalakeId, @Param("key") String key);

  /** Returns only term/metalake IDs, or null if the term or requested parent name is absent. */
  @Nullable
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "getIds")
  GlossaryTermPO getIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") String key);

  /** Returns normalized names and IDs of live terms matching a nonempty batch of keys. */
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "idsByKeys")
  List<GlossaryTermPO> idsByKeys(
      @Param("metalakeId") long metalakeId, @Param("keys") List<String> keys);

  /** Executes insert within the current transaction. */
  @InsertProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "insert")
  void insert(@Param("term") GlossaryTermPO term);
  /** Executes update within the current transaction. */
  @UpdateProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "update")
  int update(@Param("term") GlossaryTermPO term, @Param("version") long version);
  /** Executes synonyms within the current transaction. */
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "synonyms")
  List<String> synonyms(@Param("termId") long termId);
  /** Executes addSynonym within the current transaction. */
  @InsertProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "addSynonym")
  void addSynonym(
      @Param("termId") long termId,
      @Param("metalakeId") long metalakeId,
      @Param("synonym") String synonym,
      @Param("key") String key);
  /** Executes clearSynonyms within the current transaction. */
  @DeleteProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "clearSynonyms")
  void clearSynonyms(@Param("termId") long termId);
  /** Executes delete within the current transaction. */
  @UpdateProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "delete")
  int delete(
      @Param("termId") long termId,
      @Param("version") long version,
      @Param("deletedAt") long deletedAt);
  /** Executes deleteSynonyms within the current transaction. */
  @UpdateProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "deleteSynonyms")
  void deleteSynonyms(@Param("termId") long termId, @Param("deletedAt") long deletedAt);
  /** Executes dropMetalake within the current transaction. */
  @UpdateProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "dropMetalake")
  void dropMetalake(@Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt);
  /** Executes dropMetalakeSynonyms within the current transaction. */
  @UpdateProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "dropMetalakeSynonyms")
  void dropMetalakeSynonyms(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt);
  /** Executes expired within the current transaction. */
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "expired")
  List<Long> expired(@Param("cutoff") long cutoff, @Param("limit") int limit);
  /** Executes purgeSynonyms within the current transaction. */
  @DeleteProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "purgeSynonyms")
  void purgeSynonyms(@Param("termId") long termId);
  /** Executes purge within the current transaction. */
  @DeleteProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "purge")
  int purge(@Param("termId") long termId);
  /** Reads definitions, synonyms, domains and the live parent name in one non-locking snapshot. */
  @SelectProvider(type = GlossaryTermMetaSQLProviderFactory.class, method = "snapshot")
  List<GlossaryTermSnapshotPO> snapshot(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") @Nullable String key);

  /** Reads definition snapshots for one bounded batch of live endpoints. */
  @SelectProvider(
      type = GlossaryTermMetaSQLProviderFactory.class,
      method = "selectTermSnapshotsByIds")
  List<GlossaryTermSnapshotPO> selectTermSnapshotsByIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("ids") List<Long> ids);
}
