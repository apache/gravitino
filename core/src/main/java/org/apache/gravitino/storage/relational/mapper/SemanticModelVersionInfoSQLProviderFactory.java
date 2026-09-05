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
import org.apache.gravitino.storage.relational.mapper.provider.base.SemanticModelVersionInfoBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.SemanticModelVersionInfoPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects database-specific SQL providers for Semantic Model snapshot creation. */
public class SemanticModelVersionInfoSQLProviderFactory {

  private static final Map<JDBCBackendType, SemanticModelVersionInfoBaseSQLProvider>
      SEMANTIC_MODEL_VERSION_INFO_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new SemanticModelVersionInfoMySQLProvider(),
              JDBCBackendType.H2, new SemanticModelVersionInfoH2Provider(),
              JDBCBackendType.POSTGRESQL, new SemanticModelVersionInfoPostgreSQLProvider());

  /** Returns the SQL provider for the configured relational backend. */
  public static SemanticModelVersionInfoBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return SEMANTIC_MODEL_VERSION_INFO_SQL_PROVIDER_MAP.get(JDBCBackendType.fromString(databaseId));
  }

  static class SemanticModelVersionInfoMySQLProvider
      extends SemanticModelVersionInfoBaseSQLProvider {}

  static class SemanticModelVersionInfoH2Provider extends SemanticModelVersionInfoBaseSQLProvider {}

  /** Provides SQL for inserting a Semantic Model version snapshot. */
  public static String insertSemanticModelVersionInfo(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO) {
    return getProvider().insertSemanticModelVersionInfo(versionInfoPO);
  }

  /** Provides SQL for inserting or overwriting a Semantic Model version snapshot. */
  public static String insertSemanticModelVersionInfoOnDuplicateKeyUpdate(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO) {
    return getProvider().insertSemanticModelVersionInfoOnDuplicateKeyUpdate(versionInfoPO);
  }
}
