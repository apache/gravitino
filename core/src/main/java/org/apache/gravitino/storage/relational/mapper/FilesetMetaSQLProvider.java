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
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class FilesetMetaSQLProvider {
  private static final Map<JDBCBackendType, FilesetMetaBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new FilesetMetaMySQLProvider(),
              JDBCBackendType.H2, new FilesetMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new FilesetMetaPostgreSQLProvider());

  public static FilesetMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class FilesetMetaMySQLProvider extends FilesetMetaBaseProvider {}

  static class FilesetMetaH2Provider extends FilesetMetaBaseProvider {}

  static class FilesetMetaPostgreSQLProvider extends FilesetMetaBaseProvider {}

  public String listFilesetPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listFilesetPOsBySchemaId(schemaId);
  }

  public String selectFilesetIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return getProvider().selectFilesetIdBySchemaIdAndName(schemaId, name);
  }

  public String selectFilesetMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("filesetName") String name) {
    return getProvider().selectFilesetMetaBySchemaIdAndName(schemaId, name);
  }

  public String selectFilesetMetaById(@Param("filesetId") Long filesetId) {
    return getProvider().selectFilesetMetaById(filesetId);
  }

  public String insertFilesetMeta(@Param("filesetMeta") FilesetPO filesetPO) {
    return getProvider().insertFilesetMeta(filesetPO);
  }

  public String insertFilesetMetaOnDuplicateKeyUpdate(@Param("filesetMeta") FilesetPO filesetPO) {
    return getProvider().insertFilesetMetaOnDuplicateKeyUpdate(filesetPO);
  }

  public String updateFilesetMeta(
      @Param("newFilesetMeta") FilesetPO newFilesetPO,
      @Param("oldFilesetMeta") FilesetPO oldFilesetPO) {
    return getProvider().updateFilesetMeta(newFilesetPO, oldFilesetPO);
  }

  public String softDeleteFilesetMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteFilesetMetasByMetalakeId(metalakeId);
  }

  public String softDeleteFilesetMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteFilesetMetasByCatalogId(catalogId);
  }

  public String softDeleteFilesetMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteFilesetMetasBySchemaId(schemaId);
  }

  public String softDeleteFilesetMetasByFilesetId(@Param("filesetId") Long filesetId) {
    return getProvider().softDeleteFilesetMetasByFilesetId(filesetId);
  }

  public String deleteFilesetMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteFilesetMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
