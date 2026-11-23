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
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermDomainRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.GlossaryTermDomainRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects glossary term domain membership statements for the configured database. */
public class GlossaryTermDomainRelSQLProviderFactory {
  private static final GlossaryTermDomainRelBaseSQLProvider BASE =
      new GlossaryTermDomainRelBaseSQLProvider();
  private static final Map<JDBCBackendType, GlossaryTermDomainRelBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, BASE, JDBCBackendType.H2, BASE, JDBCBackendType.POSTGRESQL, BASE);

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#insert}. */
  public static String insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("domainId") long domainId) {
    return getProvider().insert(metalakeId, termId, domainId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#remove}. */
  public static String remove(
      @Param("termId") long termId,
      @Param("domainId") long domainId,
      @Param("deletedAt") long deletedAt) {
    return getProvider().remove(termId, domainId, deletedAt);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#removeTerm}. */
  public static String removeTerm(
      @Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return getProvider().removeTerm(termId, deletedAt);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#dropMetalake}. */
  public static String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return getProvider().dropMetalake(metalakeId, deletedAt);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#lastDeletedAt}. */
  public static String lastDeletedAt(@Param("termId") long termId) {
    return getProvider().lastDeletedAt(termId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#lastDeletedAtInMetalake}. */
  public static String lastDeletedAtInMetalake(@Param("metalakeId") long metalakeId) {
    return getProvider().lastDeletedAtInMetalake(metalakeId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listDomainNames}. */
  public static String listDomainNames(
      @Param("metalakeId") long metalakeId, @Param("termIds") Collection<Long> termIds) {
    return getProvider().listDomainNames(metalakeId, termIds);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listDomainIds}. */
  public static String listDomainIds(@Param("termId") long termId) {
    return getProvider().listDomainIds(termId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listTermIds}. */
  public static String listTermIds(
      @Param("metalakeId") long metalakeId, @Param("domainIds") Collection<Long> domainIds) {
    return getProvider().listTermIds(metalakeId, domainIds);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#countTerms}. */
  public static String countTerms(@Param("domainId") long domainId) {
    return getProvider().countTerms(domainId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listSubdomainIds}. */
  public static String listSubdomainIds(
      @Param("metalakeId") long metalakeId, @Param("domainId") long domainId) {
    return getProvider().listSubdomainIds(metalakeId, domainId);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#expired}. */
  public static String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return getProvider().expired(cutoff, limit);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#purge}. */
  public static String purge(@Param("row") GlossaryTermDomainRelPO row) {
    return getProvider().purge(row);
  }

  private static GlossaryTermDomainRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }
}
