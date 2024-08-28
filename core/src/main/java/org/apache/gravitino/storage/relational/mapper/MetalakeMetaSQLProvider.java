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

import static org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper.TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL Provider for Metalake Meta operations. */
public class MetalakeMetaSQLProvider {
  private static final Map<JDBCBackendType, MetalakeMetaBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new MetalakeMetaMySQLProvider(),
              JDBCBackendType.H2, new MetalakeMetaH2Provider(),
              JDBCBackendType.PG, new MetalakeMetaPGProvider());

  public static MetalakeMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class MetalakeMetaMySQLProvider extends MetalakeMetaBaseProvider {}

  static class MetalakeMetaH2Provider extends MetalakeMetaBaseProvider {}

  static class MetalakeMetaPGProvider extends MetalakeMetaBaseProvider {

    @Override
    public String softDeleteMetalakeMetaByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }
  }

  public String listMetalakePOs() {
    return getProvider().listMetalakePOs();
  }

  public String selectMetalakeMetaByName(@Param("metalakeName") String metalakeName) {
    return getProvider().selectMetalakeMetaByName(metalakeName);
  }

  public String selectMetalakeMetaById(@Param("metalakeId") Long metalakeId) {
    return getProvider().selectMetalakeMetaById(metalakeId);
  }

  public String selectMetalakeIdMetaByName(@Param("metalakeName") String metalakeName) {
    return getProvider().selectMetalakeIdMetaByName(metalakeName);
  }

  public String insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO) {
    return getProvider().insertMetalakeMeta(metalakePO);
  }

  public String insertMetalakeMetaOnDuplicateKeyUpdate(
      @Param("metalakeMeta") MetalakePO metalakePO) {
    return getProvider().insertMetalakeMetaOnDuplicateKeyUpdate(metalakePO);
  }

  public String updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO) {
    return getProvider().updateMetalakeMeta(newMetalakePO, oldMetalakePO);
  }

  public String softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteMetalakeMetaByMetalakeId(metalakeId);
  }

  public String deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
