package org.apache.gravitino.storage.relational.mapper;
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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.FilesetVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.FilesetVersionPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class FilesetVersionSQLProviderFactory {
  private static final Map<JDBCBackendType, FilesetVersionBaseSQLProvider>
      FILESET_VERSION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new FilesetVersionMySQLProvider(),
              JDBCBackendType.H2, new FilesetVersionH2Provider(),
              JDBCBackendType.POSTGRESQL, new FilesetVersionPostgreSQLProvider());

  public static FilesetVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return FILESET_VERSION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class FilesetVersionMySQLProvider extends FilesetVersionBaseSQLProvider {}

  static class FilesetVersionH2Provider extends FilesetVersionBaseSQLProvider {}

  public static String insertFilesetVersion(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO) {
    return getProvider().insertFilesetVersion(filesetVersionPO);
  }

  public static String insertFilesetVersionOnDuplicateKeyUpdate(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO) {
    return getProvider().insertFilesetVersionOnDuplicateKeyUpdate(filesetVersionPO);
  }

  public static String softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteFilesetVersionsByMetalakeId(metalakeId);
  }

  public static String softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteFilesetVersionsByCatalogId(catalogId);
  }

  public static String softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteFilesetVersionsBySchemaId(schemaId);
  }

  public static String softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId) {
    return getProvider().softDeleteFilesetVersionsByFilesetId(filesetId);
  }

  public static String deleteFilesetVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteFilesetVersionsByLegacyTimeline(legacyTimeline, limit);
  }

  public static String selectFilesetVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return getProvider().selectFilesetVersionsByRetentionCount(versionRetentionCount);
  }

  public static String softDeleteFilesetVersionsByRetentionLine(
      @Param("filesetId") Long filesetId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return getProvider()
        .softDeleteFilesetVersionsByRetentionLine(filesetId, versionRetentionLine, limit);
  }
}
