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

package org.apache.gravitino.idp.storage.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** Resolves JDBC-backend-specific SQL providers for IdP mapper factories. */
final class SQLProviderFactoryHelper {
  private SQLProviderFactoryHelper() {}

  static <T> ProviderMap<T> providerMap(
      Class<?> factoryClass, T mysqlProvider, T h2Provider, T postgresqlProvider) {
    return new ProviderMap<>(factoryClass, mysqlProvider, h2Provider, postgresqlProvider);
  }

  static final class ProviderMap<T> {
    private final Map<JDBCBackendType, T> providerMap;
    private final Class<?> factoryClass;

    private ProviderMap(
        Class<?> factoryClass, T mysqlProvider, T h2Provider, T postgresqlProvider) {
      this.factoryClass = factoryClass;
      this.providerMap =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, mysqlProvider,
              JDBCBackendType.H2, h2Provider,
              JDBCBackendType.POSTGRESQL, postgresqlProvider);
    }

    /** Returns the SQL provider for the current MyBatis database id. */
    T currentProvider() {
      return resolveProvider(currentDatabaseId(), providerMap, factoryClass);
    }

    /** Returns the SQL provider for the given MyBatis database id. */
    T getProvider(String databaseId) {
      return resolveProvider(databaseId, providerMap, factoryClass);
    }
  }

  private static <T> T resolveProvider(
      String databaseId, Map<JDBCBackendType, T> providerMap, Class<?> providerFactoryClass) {
    if (databaseId == null) {
      throw new IllegalStateException(
          String.format(
              "MyBatis databaseId is not configured for %s.",
              providerFactoryClass.getSimpleName()));
    }

    try {
      JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
      T provider = providerMap.get(jdbcBackendType);
      if (provider != null) {
        return provider;
      }

      throw new IllegalStateException(
          String.format(
              "No %s registered for backend %s (databaseId: %s)",
              providerFactoryClass.getSimpleName(), jdbcBackendType, databaseId));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format(
              "Unsupported %s databaseId: %s, supported backends: %s",
              providerFactoryClass.getSimpleName(), databaseId, providerMap.keySet()),
          e);
    }
  }

  static String currentDatabaseId() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getDatabaseId();
  }
}
