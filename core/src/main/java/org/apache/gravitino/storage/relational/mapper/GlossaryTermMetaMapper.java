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
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermSQLProvider;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.gravitino.storage.relational.po.GlossaryTermSnapshotPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** Glossary definition and synonym persistence in the caller transaction. */
public interface GlossaryTermMetaMapper {
  /** Locks the live definition, or returns null; the caller must fence its metalake first. */
  @Nullable
  @SelectProvider(type = GlossaryTermSQLProvider.class, method = "getForUpdate")
  GlossaryTermPO getForUpdate(@Param("metalakeId") long metalakeId, @Param("key") String key);

  /** Returns only term/metalake IDs, or null if the term or requested parent name is absent. */
  @Nullable
  @SelectProvider(type = GlossaryTermSQLProvider.class, method = "getIds")
  GlossaryTermPO getIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") String key);

  /** Executes insert within the current transaction. */
  @InsertProvider(type = GlossaryTermSQLProvider.class, method = "insert")
  void insert(@Param("p") GlossaryTermPO p);
  /** Executes update within the current transaction. */
  @UpdateProvider(type = GlossaryTermSQLProvider.class, method = "update")
  int update(@Param("p") GlossaryTermPO p, @Param("version") long version);
  /** Executes synonyms within the current transaction. */
  @SelectProvider(type = GlossaryTermSQLProvider.class, method = "synonyms")
  List<String> synonyms(@Param("id") long id);
  /** Executes addSynonym within the current transaction. */
  @InsertProvider(type = GlossaryTermSQLProvider.class, method = "addSynonym")
  void addSynonym(
      @Param("id") long id,
      @Param("metalakeId") long metalakeId,
      @Param("synonym") String synonym,
      @Param("key") String key);
  /** Executes clearSynonyms within the current transaction. */
  @DeleteProvider(type = GlossaryTermSQLProvider.class, method = "clearSynonyms")
  void clearSynonyms(@Param("id") long id);
  /** Executes delete within the current transaction. */
  @UpdateProvider(type = GlossaryTermSQLProvider.class, method = "delete")
  int delete(
      @Param("id") long id, @Param("version") long version, @Param("deletedAt") long deletedAt);
  /** Executes deleteSynonyms within the current transaction. */
  @UpdateProvider(type = GlossaryTermSQLProvider.class, method = "deleteSynonyms")
  void deleteSynonyms(@Param("id") long id, @Param("deletedAt") long deletedAt);
  /** Executes dropMetalake within the current transaction. */
  @UpdateProvider(type = GlossaryTermSQLProvider.class, method = "dropMetalake")
  void dropMetalake(@Param("id") long id, @Param("deletedAt") long deletedAt);
  /** Executes dropMetalakeSynonyms within the current transaction. */
  @UpdateProvider(type = GlossaryTermSQLProvider.class, method = "dropMetalakeSynonyms")
  void dropMetalakeSynonyms(@Param("id") long id, @Param("deletedAt") long deletedAt);
  /** Executes expired within the current transaction. */
  @SelectProvider(type = GlossaryTermSQLProvider.class, method = "expired")
  List<Long> expired(@Param("cutoff") long cutoff, @Param("limit") int limit);
  /** Executes purgeSynonyms within the current transaction. */
  @DeleteProvider(type = GlossaryTermSQLProvider.class, method = "purgeSynonyms")
  void purgeSynonyms(@Param("id") long id);
  /** Executes purge within the current transaction. */
  @DeleteProvider(type = GlossaryTermSQLProvider.class, method = "purge")
  int purge(@Param("id") long id);
  /** Reads definitions, synonyms and the live parent name in one snapshot without row locks. */
  @SelectProvider(type = GlossaryTermSQLProvider.class, method = "snapshot")
  List<GlossaryTermSnapshotPO> snapshot(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") @Nullable String key);
}
