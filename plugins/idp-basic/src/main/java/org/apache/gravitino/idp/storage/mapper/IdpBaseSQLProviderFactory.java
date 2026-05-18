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

abstract class IdpBaseSQLProviderFactory<T> {
  private final T mysqlProvider;
  private final T h2Provider;
  private final T postgresqlProvider;
  private final Map<JDBCBackendType, T> providerMap;

  protected IdpBaseSQLProviderFactory(T mysqlProvider, T h2Provider, T postgresqlProvider) {
    this.mysqlProvider = mysqlProvider;
    this.h2Provider = h2Provider;
    this.postgresqlProvider = postgresqlProvider;
    this.providerMap =
        ImmutableMap.of(
            JDBCBackendType.MYSQL,
            mysqlProvider,
            JDBCBackendType.H2,
            h2Provider,
            JDBCBackendType.POSTGRESQL,
            postgresqlProvider);
  }

  protected final T h2ProviderInstance() {
    return h2Provider;
  }

  protected final T mysqlProviderInstance() {
    return mysqlProvider;
  }

  protected final T postgresqlProviderInstance() {
    return postgresqlProvider;
  }

  protected final T currentProviderInstance() {
    return resolveProvider(currentDatabaseId());
  }

  protected final T resolveProvider(String databaseId) {
    if (databaseId == null) {
      throw new IllegalStateException(
          String.format(
              "MyBatis databaseId is not configured for %s.", getClass().getSimpleName()));
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
              getClass().getSimpleName(), jdbcBackendType, databaseId));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format(
              "Unsupported %s databaseId: %s, supported backends: %s",
              getClass().getSimpleName(), databaseId, providerMap.keySet()),
          e);
    }
  }

  private String currentDatabaseId() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getDatabaseId();
  }
}
