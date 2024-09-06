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

  static class MetalakeMetaPostgreSQLProvider extends MetalakeMetaBaseSQLProvider {

    @Override
    public String softDeleteMetalakeMetaByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String insertMetalakeMetaOnDuplicateKeyUpdate(MetalakePO metalakePO) {
      return "INSERT INTO "
          + TABLE_NAME
          + "(metalake_id, metalake_name, metalake_comment, properties, audit_info,"
          + " schema_version, current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{metalakeMeta.metalakeId},"
          + " #{metalakeMeta.metalakeName},"
          + " #{metalakeMeta.metalakeComment},"
          + " #{metalakeMeta.properties},"
          + " #{metalakeMeta.auditInfo},"
          + " #{metalakeMeta.schemaVersion},"
          + " #{metalakeMeta.currentVersion},"
          + " #{metalakeMeta.lastVersion},"
          + " #{metalakeMeta.deletedAt}"
          + " )"
          + " ON CONFLICT(metalake_id) DO UPDATE SET"
          + " metalake_name = #{metalakeMeta.metalakeName},"
          + " metalake_comment = #{metalakeMeta.metalakeComment},"
          + " properties = #{metalakeMeta.properties},"
          + " audit_info = #{metalakeMeta.auditInfo},"
          + " schema_version = #{metalakeMeta.schemaVersion},"
          + " current_version = #{metalakeMeta.currentVersion},"
          + " last_version = #{metalakeMeta.lastVersion},"
          + " deleted_at = #{metalakeMeta.deletedAt}";
    }

    public String updateMetalakeMeta(
        @Param("newMetalakeMeta") MetalakePO newMetalakePO,
        @Param("oldMetalakeMeta") MetalakePO oldMetalakePO) {
      return "UPDATE "
          + TABLE_NAME
          + " SET metalake_name = #{newMetalakeMeta.metalakeName},"
          + " metalake_comment = #{newMetalakeMeta.metalakeComment},"
          + " properties = #{newMetalakeMeta.properties},"
          + " audit_info = #{newMetalakeMeta.auditInfo},"
          + " schema_version = #{newMetalakeMeta.schemaVersion},"
          + " current_version = #{newMetalakeMeta.currentVersion},"
          + " last_version = #{newMetalakeMeta.lastVersion}"
          + " WHERE metalake_id = #{oldMetalakeMeta.metalakeId}"
          + " AND metalake_name = #{oldMetalakeMeta.metalakeName}"
          + " AND (metalake_comment = #{oldMetalakeMeta.metalakeComment} "
          + "  OR (CAST(metalake_comment AS VARCHAR) IS NULL AND "
          + "  CAST(#{oldMetalakeMeta.metalakeComment} AS VARCHAR) IS NULL))"
          + " AND properties = #{oldMetalakeMeta.properties}"
          + " AND audit_info = #{oldMetalakeMeta.auditInfo}"
          + " AND schema_version = #{oldMetalakeMeta.schemaVersion}"
          + " AND current_version = #{oldMetalakeMeta.currentVersion}"
          + " AND last_version = #{oldMetalakeMeta.lastVersion}"
          + " AND deleted_at = 0";
    }
  }

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
