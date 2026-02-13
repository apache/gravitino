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
import org.apache.gravitino.storage.relational.mapper.provider.base.CacheInvalidationVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL provider factory for cache invalidation version operations. */
public class CacheInvalidationVersionSQLProviderFactory {

  private static final Map<JDBCBackendType, CacheInvalidationVersionBaseSQLProvider> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new CacheInvalidationVersionBaseSQLProvider(),
          JDBCBackendType.H2, new CacheInvalidationVersionBaseSQLProvider(),
          JDBCBackendType.POSTGRESQL, new CacheInvalidationVersionBaseSQLProvider());

  private static CacheInvalidationVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String selectVersion(@Param("id") Long id) {
    return getProvider().selectVersion(id);
  }

  public static String insertVersion(
      @Param("id") Long id, @Param("version") Long version, @Param("updatedAt") Long updatedAt) {
    return getProvider().insertVersion(id, version, updatedAt);
  }

  public static String incrementVersion(@Param("id") Long id, @Param("updatedAt") Long updatedAt) {
    return getProvider().incrementVersion(id, updatedAt);
  }
}
