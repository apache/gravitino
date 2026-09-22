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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.OwnerMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.OwnerMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.OwnerRelForDeletion;
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

  /** Returns SQL that locks an active metadata object before assigning its owner. */
  public static String selectMetadataObjectIdForUpdate(
      @Param("entityId") Long entityId,
      @Param("metalakeId") Long metalakeId,
      @Param("entityType") Entity.EntityType entityType) {
    String table;
    String idColumn;
    switch (entityType) {
      case CATALOG:
        table = CatalogMetaMapper.TABLE_NAME;
        idColumn = "catalog_id";
        break;
      case SCHEMA:
        table = SchemaMetaMapper.TABLE_NAME;
        idColumn = "schema_id";
        break;
      case TABLE:
        table = TableMetaMapper.TABLE_NAME;
        idColumn = "table_id";
        break;
      case COLUMN:
        table = TableColumnMapper.COLUMN_TABLE_NAME;
        idColumn = "column_id";
        break;
      case FILESET:
        table = FilesetMetaMapper.META_TABLE_NAME;
        idColumn = "fileset_id";
        break;
      case TOPIC:
        table = TopicMetaMapper.TABLE_NAME;
        idColumn = "topic_id";
        break;
      case MODEL:
        table = ModelMetaMapper.TABLE_NAME;
        idColumn = "model_id";
        break;
      case VIEW:
        table = ViewMetaMapper.TABLE_NAME;
        idColumn = "view_id";
        break;
      case FUNCTION:
        table = FunctionMetaMapper.TABLE_NAME;
        idColumn = "function_id";
        break;
      case ROLE:
        table = RoleMetaMapper.ROLE_TABLE_NAME;
        idColumn = "role_id";
        break;
      case TAG:
        table = TagMetaMapper.TAG_TABLE_NAME;
        idColumn = "tag_id";
        break;
      case POLICY:
        table = PolicyMetaMapper.POLICY_META_TABLE_NAME;
        idColumn = "policy_id";
        break;
      case JOB_TEMPLATE:
        table = JobTemplateMetaMapper.TABLE_NAME;
        idColumn = "job_template_id";
        break;
      case JOB:
        table = JobMetaMapper.TABLE_NAME;
        idColumn = "job_run_id";
        break;
      default:
        throw new IllegalArgumentException("Unsupported owned object type: " + entityType);
    }
    // Column versions can share a column ID; always lock the same oldest live row.
    String orderColumn = entityType == Entity.EntityType.COLUMN ? "id" : idColumn;
    return "SELECT "
        + idColumn
        + " FROM "
        + table
        + " WHERE "
        + idColumn
        + " = #{entityId} AND metalake_id = #{metalakeId} AND deleted_at = 0"
        + " ORDER BY "
        + orderColumn
        + " LIMIT 1 FOR UPDATE";
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

  public static String batchInsertOwnerRels(@Param("ownerRelPOs") List<OwnerRelPO> ownerRelPOs) {
    return getProvider().batchInsertOwnerRels(ownerRelPOs);
  }

  public static String batchSoftDeleteOwnerRelByMetadataObjects(
      @Param("deletions") List<OwnerRelForDeletion> deletions) {
    return getProvider().batchSoftDeleteOwnerRelByMetadataObjects(deletions);
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

  public static String softDeleteOwnerRelBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return getProvider().softDeleteOwnerRelBySchemaIds(schemaIds);
  }

  public static String deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteOwnerMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String batchSelectUserOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectIds") List<Long> metadataObjectIds,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .batchSelectUserOwnerMetaByMetadataObjectIdAndType(metadataObjectIds, metadataObjectType);
  }

  /**
   * Builds SQL to select group owners for the specified metadata objects.
   *
   * @param metadataObjectIds IDs of the metadata objects
   * @param metadataObjectType type of the metadata objects
   * @return SQL for selecting group owners
   */
  public static String batchSelectGroupOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectIds") List<Long> metadataObjectIds,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .batchSelectGroupOwnerMetaByMetadataObjectIdAndType(metadataObjectIds, metadataObjectType);
  }

  public static String selectOwnerByMetadataObjectIdAndType(
      @Param("metadataObjectId") long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider().selectOwnerByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String selectChangedOwners(
      @Param("lastConsumedUpdatedAt") long lastConsumedUpdatedAt,
      @Param("lastConsumedUpdatedAtId") long lastConsumedUpdatedAtId) {
    return getProvider().selectChangedOwners(lastConsumedUpdatedAt, lastConsumedUpdatedAtId);
  }

  public static String selectMaxChangedOwner() {
    return getProvider().selectMaxChangedOwner();
  }
}
