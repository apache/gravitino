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
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermBindingBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.GlossaryTermBindingPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects glossary term binding statements for the configured database. */
public class GlossaryTermBindingSQLProviderFactory {
  private static final GlossaryTermBindingBaseSQLProvider BASE =
      new GlossaryTermBindingBaseSQLProvider();
  private static final Map<JDBCBackendType, GlossaryTermBindingBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, BASE, JDBCBackendType.H2, BASE, JDBCBackendType.POSTGRESQL, BASE);

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#insert}. */
  public static String insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId) {
    return getProvider().insert(metalakeId, termId, targetType, targetId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#remove}. */
  public static String remove(
      @Param("termId") long termId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId,
      @Param("deletedAt") long deletedAt) {
    return getProvider().remove(termId, targetType, targetId, deletedAt);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#removeTerm}. */
  public static String removeTerm(
      @Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return getProvider().removeTerm(termId, deletedAt);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#dropMetalake}. */
  public static String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return getProvider().dropMetalake(metalakeId, deletedAt);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#lastDeletedAt}. */
  public static String lastDeletedAt(@Param("termId") long termId) {
    return getProvider().lastDeletedAt(termId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#lastDeletedAtInMetalake}. */
  public static String lastDeletedAtInMetalake(@Param("metalakeId") long metalakeId) {
    return getProvider().lastDeletedAtInMetalake(metalakeId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listTermIds}. */
  public static String listTermIds(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetId") long targetId) {
    return getProvider().listTermIds(metalakeId, targetType, targetId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listByTargets}. */
  public static String listByTargets(
      @Param("metalakeId") long metalakeId,
      @Param("targetType") String targetType,
      @Param("targetIds") Collection<Long> targetIds) {
    return getProvider().listByTargets(metalakeId, targetType, targetIds);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listByTerm}. */
  public static String listByTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("targetTypes") @Nullable Collection<String> targetTypes) {
    return getProvider().listByTerm(metalakeId, termId, targetTypes);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listByTable}. */
  public static String listByTable(
      @Param("metalakeId") long metalakeId, @Param("tableId") long tableId) {
    return getProvider().listByTable(metalakeId, tableId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listBySemanticModel}. */
  public static String listBySemanticModel(
      @Param("metalakeId") long metalakeId, @Param("semanticModelId") long semanticModelId) {
    return getProvider().listBySemanticModel(metalakeId, semanticModelId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listBySchemas}. */
  public static String listBySchemas(
      @Param("metalakeId") long metalakeId, @Param("schemaIds") Collection<Long> schemaIds) {
    return getProvider().listBySchemas(metalakeId, schemaIds);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#listByCatalog}. */
  public static String listByCatalog(
      @Param("metalakeId") long metalakeId, @Param("catalogId") long catalogId) {
    return getProvider().listByCatalog(metalakeId, catalogId);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#expired}. */
  public static String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return getProvider().expired(cutoff, limit);
  }

  /** Delegates {@link GlossaryTermBindingBaseSQLProvider#purge}. */
  public static String purge(@Param("row") GlossaryTermBindingPO row) {
    return getProvider().purge(row);
  }

  private static GlossaryTermBindingBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }
}
