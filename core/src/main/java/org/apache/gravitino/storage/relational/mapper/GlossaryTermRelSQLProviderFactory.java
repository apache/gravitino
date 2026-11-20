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
import java.util.Collection;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.GlossaryTermRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects glossary relation and hierarchy lock statements for the configured database. */
public class GlossaryTermRelSQLProviderFactory {
  private static final Map<JDBCBackendType, GlossaryTermRelBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new GlossaryTermRelBaseSQLProvider(),
          JDBCBackendType.H2, new GlossaryTermRelH2Provider(),
          JDBCBackendType.POSTGRESQL, new GlossaryTermRelPostgreSQLProvider());

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#ensureLock}. */
  public static String ensureLock(@Param("metalakeId") long metalakeId) {
    return getProvider().ensureLock(metalakeId);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#lockHierarchy}. */
  public static String lockHierarchy(@Param("metalakeId") long metalakeId) {
    return getProvider().lockHierarchy(metalakeId);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#lockTerm}. */
  public static String lockTerm(
      @Param("metalakeId") long metalakeId, @Param("termId") long termId) {
    return getProvider().lockTerm(metalakeId, termId);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#insert}. */
  public static String insert(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type) {
    return getProvider().insert(metalakeId, fromId, toId, type);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#exists}. */
  public static String exists(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type) {
    return getProvider().exists(metalakeId, fromId, toId, type);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#remove}. */
  public static String remove(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type,
      @Param("deletedAt") long deletedAt) {
    return getProvider().remove(metalakeId, fromId, toId, type, deletedAt);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#lastDeletedAt}. */
  public static String lastDeletedAt(@Param("metalakeId") long metalakeId) {
    return getProvider().lastDeletedAt(metalakeId);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#removeTerm}. */
  public static String removeTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("deletedAt") long deletedAt) {
    return getProvider().removeTerm(metalakeId, termId, deletedAt);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#dropMetalake}. */
  public static String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return getProvider().dropMetalake(metalakeId, deletedAt);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#dropLock}. */
  public static String dropLock(@Param("metalakeId") long metalakeId) {
    return getProvider().dropLock(metalakeId);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#listEdges}. */
  public static String listEdges(
      @Param("metalakeId") long metalakeId,
      @Param("ids") Collection<Long> ids,
      @Param("type") String type,
      @Param("reverse") boolean reverse) {
    return getProvider().listEdges(metalakeId, ids, type, reverse);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#expired}. */
  public static String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return getProvider().expired(cutoff, limit);
  }

  /** Delegates {@link GlossaryTermRelBaseSQLProvider#purge}. */
  public static String purge(@Param("relation") GlossaryTermRelPO relation) {
    return getProvider().purge(relation);
  }

  private static GlossaryTermRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  static class GlossaryTermRelH2Provider extends GlossaryTermRelBaseSQLProvider {
    @Override
    public String ensureLock(@Param("metalakeId") long metalakeId) {
      return "MERGE INTO glossary_hierarchy_lock (metalake_id) KEY (metalake_id) "
          + "VALUES (#{metalakeId})";
    }
  }
}
