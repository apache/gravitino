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

import static org.apache.gravitino.storage.relational.mapper.OwnerMetaMapper.OWNER_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class OwnerMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, OwnerMetaBaseSQLProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
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
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class OwnerMetaMySQLProvider extends OwnerMetaBaseSQLProvider {}

  static class OwnerMetaH2Provider extends OwnerMetaBaseSQLProvider {}

  static class OwnerMetaPostgreSQLProvider extends OwnerMetaBaseSQLProvider {

    @Override
    public String softDeleteOwnerRelByMetadataObjectIdAndType(
        Long metadataObjectId, String metadataObjectType) {
      return "UPDATE "
          + OWNER_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metadata_object_id = #{metadataObjectId} AND metadata_object_type = #{metadataObjectType} AND deleted_at = 0";
    }

    @Override
    public String softDeleteOwnerRelByOwnerIdAndType(Long ownerId, String ownerType) {
      return "UPDATE "
          + OWNER_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE owner_id = #{ownerId} AND owner_type = #{ownerType} AND deleted_at = 0";
    }

    @Override
    public String softDeleteOwnerRelByMetalakeId(Long metalakeId) {
      return "UPDATE  "
          + OWNER_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at =0";
    }

    @Override
    public String softDeleteOwnerRelByCatalogId(Long catalogId) {
      return "UPDATE  "
          + OWNER_TABLE_NAME
          + " ot SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE EXISTS ("
          + " SELECT ct.catalog_id FROM "
          + CatalogMetaMapper.TABLE_NAME
          + " ct WHERE ct.catalog_id = #{catalogId}  AND ct.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "ct.catalog_id = ot.metadata_object_id AND ot.metadata_object_type = 'CATALOG'"
          + " UNION "
          + " SELECT st.catalog_id FROM "
          + SchemaMetaMapper.TABLE_NAME
          + " st WHERE st.catalog_id = #{catalogId} AND st.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
          + " UNION "
          + " SELECT tt.catalog_id FROM "
          + TopicMetaMapper.TABLE_NAME
          + " tt WHERE tt.catalog_id = #{catalogId} AND tt.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
          + " UNION "
          + " SELECT tat.catalog_id FROM "
          + TableMetaMapper.TABLE_NAME
          + " tat WHERE tat.catalog_id = #{catalogId} AND tat.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
          + " UNION "
          + " SELECT ft.catalog_id FROM "
          + FilesetMetaMapper.META_TABLE_NAME
          + " ft WHERE ft.catalog_id = #{catalogId} AND ft.deleted_at = 0 AND ot.deleted_at = 0 AND"
          + " ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
          + ")";
    }

    @Override
    public String sotDeleteOwnerRelBySchemaId(Long schemaId) {
      return "UPDATE  "
          + OWNER_TABLE_NAME
          + " ot SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE EXISTS ("
          + " SELECT st.schema_id FROM "
          + SchemaMetaMapper.TABLE_NAME
          + " st WHERE st.schema_id = #{schemaId} AND st.deleted_at = 0 AND ot.deleted_at = 0 "
          + "AND st.schema_id = ot.metadata_object_id AND ot.metadata_object_type = 'SCHEMA'"
          + " UNION "
          + " SELECT tt.schema_id FROM "
          + TopicMetaMapper.TABLE_NAME
          + " tt WHERE tt.schema_id = #{schemaId} AND tt.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "tt.topic_id = ot.metadata_object_id AND ot.metadata_object_type = 'TOPIC'"
          + " UNION "
          + " SELECT tat.schema_id FROM "
          + TableMetaMapper.TABLE_NAME
          + " tat WHERE tat.schema_id = #{schemaId} AND tat.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "tat.table_id = ot.metadata_object_id AND ot.metadata_object_type = 'TABLE'"
          + " UNION "
          + " SELECT ft.schema_id FROM "
          + FilesetMetaMapper.META_TABLE_NAME
          + " ft WHERE ft.schema_id = #{schemaId} AND ft.deleted_at = 0 AND ot.deleted_at = 0 AND "
          + "ft.fileset_id = ot.metadata_object_id AND ot.metadata_object_type = 'FILESET'"
          + ")";
    }
  }

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
