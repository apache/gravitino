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
import org.apache.gravitino.storage.relational.mapper.provider.base.MetalakeMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.MetalakeMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL Provider for Metalake Meta operations. */
public class MetalakeMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, MetalakeMetaBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new MetalakeMetaMySQLProvider(),
              JDBCBackendType.H2, new MetalakeMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new MetalakeMetaPostgreSQLProvider());

  public static MetalakeMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class MetalakeMetaMySQLProvider extends MetalakeMetaBaseSQLProvider {}

  static class MetalakeMetaH2Provider extends MetalakeMetaBaseSQLProvider {}

  public String listMetalakePOs() {
    return getProvider().listMetalakePOs();
  }

  public static String selectMetalakeMetaByName(@Param("metalakeName") String metalakeName) {
    return getProvider().selectMetalakeMetaByName(metalakeName);
  }

  public static String selectMetalakeMetaById(@Param("metalakeId") Long metalakeId) {
    return getProvider().selectMetalakeMetaById(metalakeId);
  }

  public static String selectMetalakeIdMetaByName(@Param("metalakeName") String metalakeName) {
    return getProvider().selectMetalakeIdMetaByName(metalakeName);
  }

  public static String insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO) {
    return getProvider().insertMetalakeMeta(metalakePO);
  }

  public static String insertMetalakeMetaOnDuplicateKeyUpdate(
      @Param("metalakeMeta") MetalakePO metalakePO) {
    return getProvider().insertMetalakeMetaOnDuplicateKeyUpdate(metalakePO);
  }

  public static String updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO) {
    return getProvider().updateMetalakeMeta(newMetalakePO, oldMetalakePO);
  }

  public static String softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteMetalakeMetaByMetalakeId(metalakeId);
  }

  public static String deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
