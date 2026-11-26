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
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** Glossary edges and hierarchy locks in the caller transaction. */
public interface GlossaryTermRelMapper {
  /** Table containing glossary term relations. */
  String TABLE_NAME = "glossary_term_rel";

  /** Table containing the hierarchy lock for each metalake. */
  String HIERARCHY_LOCK_TABLE_NAME = "glossary_hierarchy_lock";

  /** Creates the mutex row safely on first use. */
  @InsertProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "ensureLock")
  void ensureLock(@Param("metalakeId") long metalakeId);

  /** Locks the existing hierarchy mutex in the caller transaction. */
  @Nullable
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "lockHierarchy")
  Long lockHierarchy(@Param("metalakeId") long metalakeId);

  /** Locks a live term in this metalake, or returns null. */
  @Nullable
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "lockTerm")
  Long lockTerm(@Param("metalakeId") long metalakeId, @Param("termId") long termId);

  /** Inserts a live canonical edge under the hierarchy mutex. */
  @InsertProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "insert")
  int insert(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type);

  /** Counts matching live edges. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "exists")
  int exists(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type);

  /** Soft-deletes the matching live edge. */
  @UpdateProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "remove")
  int remove(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type,
      @Param("deletedAt") long deletedAt);

  /** Returns the greatest retained deletion timestamp, or zero. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "lastDeletedAt")
  long lastDeletedAt(@Param("metalakeId") long metalakeId);

  /** Retires all edges touching a deleted term. */
  @UpdateProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "removeTerm")
  void removeTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("deletedAt") long deletedAt);

  /** Retires all edges of a deleted metalake. */
  @UpdateProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "dropMetalake")
  void dropMetalake(@Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt);

  /** Removes the mutex under the exclusive metalake deletion fence. */
  @DeleteProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "dropLock")
  void dropLock(@Param("metalakeId") long metalakeId);

  /** Reads one nonempty frontier batch with live endpoints in a single statement. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "listEdges")
  List<GlossaryTermRelPO> listEdges(
      @Param("metalakeId") long metalakeId,
      @Param("ids") Collection<Long> ids,
      @Param("type") String type,
      @Param("reverse") boolean reverse,
      @Param("status") @Nullable String status,
      @Param("limit") @Nullable Integer limit);

  /** Selects a bounded batch of expired generations. */
  @Options(useCache = false, flushCache = FlushCachePolicy.TRUE)
  @SelectProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "expired")
  List<GlossaryTermRelPO> expired(@Param("cutoff") long cutoff, @Param("limit") int limit);

  /** Deletes the exact expired generation, not the live replacement. */
  @DeleteProvider(type = GlossaryTermRelSQLProviderFactory.class, method = "purge")
  int purge(@Param("relation") GlossaryTermRelPO relation);
}
