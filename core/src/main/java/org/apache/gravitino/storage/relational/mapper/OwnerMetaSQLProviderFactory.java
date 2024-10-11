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
import org.apache.gravitino.storage.relational.mapper.provider.base.OwnerMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.OwnerMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class OwnerMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, OwnerMetaBaseSQLProvider> OWNER_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new OwnerMetaMySQLProvider(),
          JDBCBackendType.H2, new OwnerMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new OwnerMetaPostgreSQLProvider());

  public static OwnerMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return OWNER_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class OwnerMetaMySQLProvider extends OwnerMetaBaseSQLProvider {}

  static class OwnerMetaH2Provider extends OwnerMetaBaseSQLProvider {}

  public static String selectUserOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .selectUserOwnerMetaByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String selectGroupOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .selectGroupOwnerMetaByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String insertOwnerRel(@Param("ownerRelPO") OwnerRelPO ownerRelPO) {
    return getProvider().insertOwnerRel(ownerRelPO);
  }

  public static String softDeleteOwnerRelByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .softDeleteOwnerRelByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String softDeleteOwnerRelByOwnerIdAndType(
      @Param("ownerId") Long ownerId, @Param("ownerType") String ownerType) {
    return getProvider().softDeleteOwnerRelByOwnerIdAndType(ownerId, ownerType);
  }

  public static String softDeleteOwnerRelByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteOwnerRelByMetalakeId(metalakeId);
  }

  public static String softDeleteOwnerRelByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteOwnerRelByCatalogId(catalogId);
  }

  public static String softDeleteOwnerRelBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().sotDeleteOwnerRelBySchemaId(schemaId);
  }

  public static String deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteOwnerMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
