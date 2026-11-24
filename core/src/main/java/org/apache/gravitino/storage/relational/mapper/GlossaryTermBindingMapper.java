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

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** Glossary term binding rows in the caller transaction. */
public interface GlossaryTermBindingMapper {
  /** Table containing glossary term bindings to metadata objects and metrics. */
  String TABLE_NAME = "glossary_term_binding";

  /** Inserts a live binding row. */
  @InsertProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "insert")
  int insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId);

  /** Retires one live binding row. */
  @UpdateProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "remove")
  int remove(
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId,
      @Param("deletedAt") long deletedAt);

  /** Retires every live binding row of a term. */
  @UpdateProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "removeTerm")
  int removeTerm(@Param("termId") long termId, @Param("deletedAt") long deletedAt);

  /** Retires every live binding row of a metalake. */
  @UpdateProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "dropMetalake")
  int dropMetalake(@Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt);

  /** Returns the latest tombstone time of a term, or 0. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "lastDeletedAt")
  long lastDeletedAt(@Param("termId") long termId);

  /** Returns the latest tombstone time in a metalake, or 0. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(
      type = GlossaryTermBindingSQLProviderFactory.class,
      method = "lastDeletedAtInMetalake")
  long lastDeletedAtInMetalake(@Param("metalakeId") long metalakeId);

  /** Lists live term IDs bound to a single target. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listTermIds")
  List<Long> listTermIds(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId);

  /** Lists live bindings for a nonempty batch of target IDs of one target type. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listByTargets")
  List<GlossaryTermBindingPO> listByTargets(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetIds") Collection<Long> targetIds);

  /** Lists live bindings of a term, optionally filtered by target types, in keyset order. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listByTerm")
  List<GlossaryTermBindingPO> listByTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetTypes") @Nullable Collection<String> targetTypes);

  /** Lists live bindings belonging to a table and all its live columns. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listByTable")
  List<GlossaryTermBindingPO> listByTable(
      @Param("metalakeId") long metalakeId, @Param("tableId") long tableId);

  /** Lists live bindings belonging to all live metrics of a semantic model. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(
      type = GlossaryTermBindingSQLProviderFactory.class,
      method = "listBySemanticModel")
  List<GlossaryTermBindingPO> listBySemanticModel(
      @Param("metalakeId") long metalakeId, @Param("semanticModelId") long semanticModelId);

  /** Lists live bindings belonging to schemas and all their live child objects and metrics. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listBySchemas")
  List<GlossaryTermBindingPO> listBySchemas(
      @Param("metalakeId") long metalakeId, @Param("schemaIds") Collection<Long> schemaIds);

  /** Lists live bindings belonging to a catalog and all its live descendant objects and metrics. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "listByCatalog")
  List<GlossaryTermBindingPO> listByCatalog(
      @Param("metalakeId") long metalakeId, @Param("catalogId") long catalogId);

  /** Selects a bounded batch of retired rows for garbage collection. */
  @SelectProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "expired")
  List<GlossaryTermBindingPO> expired(@Param("cutoff") long cutoff, @Param("limit") int limit);

  /** Deletes one selected retired row. */
  @DeleteProvider(type = GlossaryTermBindingSQLProviderFactory.class, method = "purge")
  int purge(@Param("row") GlossaryTermBindingPO row);
}
