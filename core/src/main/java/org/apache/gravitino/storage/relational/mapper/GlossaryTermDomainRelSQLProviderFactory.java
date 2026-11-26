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

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listDomainNames}. */
  public static String listDomainNames(
      @Param("metalakeId") long metalakeId, @Param("termIds") Collection<Long> termIds) {
    return getProvider().listDomainNames(metalakeId, termIds);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#listTermIds}. */
  public static String listTermIds(
      @Param("metalakeId") long metalakeId, @Param("domainIds") Collection<Long> domainIds) {
    return getProvider().listTermIds(metalakeId, domainIds);
  }

  /** Delegates {@link GlossaryTermDomainRelBaseSQLProvider#countLiveMemberships}. */
  public static String countLiveMemberships() {
    return getProvider().countLiveMemberships();
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
