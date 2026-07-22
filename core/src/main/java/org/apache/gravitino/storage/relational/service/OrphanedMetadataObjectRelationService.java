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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FunctionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OrphanedMetadataObjectRelationMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.RoleMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Collects relation rows whose referenced metadata object is no longer live. */
public class OrphanedMetadataObjectRelationService {
  private static final OrphanedMetadataObjectRelationService INSTANCE =
      new OrphanedMetadataObjectRelationService();

  private static final Map<MetadataObject.Type, EntityTable> ENTITY_TABLES =
      ImmutableMap.<MetadataObject.Type, EntityTable>builder()
          .put(
              MetadataObject.Type.METALAKE,
              new EntityTable(MetalakeMetaMapper.TABLE_NAME, "metalake_id"))
          .put(MetadataObject.Type.CATALOG, new EntityTable(TABLE_NAME, "catalog_id"))
          .put(
              MetadataObject.Type.SCHEMA, new EntityTable(SchemaMetaMapper.TABLE_NAME, "schema_id"))
          .put(
              MetadataObject.Type.FILESET,
              new EntityTable(FilesetMetaMapper.META_TABLE_NAME, "fileset_id"))
          .put(MetadataObject.Type.TABLE, new EntityTable(TableMetaMapper.TABLE_NAME, "table_id"))
          .put(MetadataObject.Type.VIEW, new EntityTable(ViewMetaMapper.TABLE_NAME, "view_id"))
          .put(MetadataObject.Type.TOPIC, new EntityTable(TopicMetaMapper.TABLE_NAME, "topic_id"))
          .put(
              MetadataObject.Type.COLUMN,
              new EntityTable(TableColumnMapper.COLUMN_TABLE_NAME, "column_id"))
          .put(MetadataObject.Type.ROLE, new EntityTable(RoleMetaMapper.ROLE_TABLE_NAME, "role_id"))
          .put(MetadataObject.Type.MODEL, new EntityTable(ModelMetaMapper.TABLE_NAME, "model_id"))
          .put(MetadataObject.Type.TAG, new EntityTable(TagMetaMapper.TAG_TABLE_NAME, "tag_id"))
          .put(
              MetadataObject.Type.POLICY,
              new EntityTable(PolicyMetaMapper.POLICY_META_TABLE_NAME, "policy_id"))
          .put(MetadataObject.Type.JOB, new EntityTable(JobMetaMapper.TABLE_NAME, "job_run_id"))
          .put(
              MetadataObject.Type.JOB_TEMPLATE,
              new EntityTable(JobTemplateMetaMapper.TABLE_NAME, "job_template_id"))
          .put(
              MetadataObject.Type.FUNCTION,
              new EntityTable(FunctionMetaMapper.TABLE_NAME, "function_id"))
          .build();

  private OrphanedMetadataObjectRelationService() {}

  /**
   * Returns the singleton service instance.
   *
   * @return singleton service instance
   */
  public static OrphanedMetadataObjectRelationService getInstance() {
    return INSTANCE;
  }

  /**
   * Soft-deletes orphaned relation rows for one metadata object type.
   *
   * @param metadataObjectType metadata object type to collect
   * @param limit maximum number of orphaned object IDs processed per relation table
   * @return number of relation rows soft-deleted
   */
  public int softDeleteOrphanedRelations(MetadataObject.Type metadataObjectType, int limit) {
    Preconditions.checkNotNull(metadataObjectType, "metadataObjectType cannot be null");
    Preconditions.checkArgument(limit > 0, "limit must be positive");
    EntityTable entityTable = ENTITY_TABLES.get(metadataObjectType);
    if (entityTable == null) {
      return 0;
    }

    long deletedAt = System.currentTimeMillis();
    return SessionUtils.doWithCommitAndFetchResult(
        OrphanedMetadataObjectRelationMapper.class,
        mapper ->
            mapper.softDeleteOrphanedOwnerRelations(
                    entityTable.tableName,
                    entityTable.idColumn,
                    metadataObjectType.name(),
                    deletedAt,
                    limit)
                + mapper.softDeleteOrphanedTagRelations(
                    entityTable.tableName,
                    entityTable.idColumn,
                    metadataObjectType.name(),
                    deletedAt,
                    limit)
                + mapper.softDeleteOrphanedPolicyRelations(
                    entityTable.tableName,
                    entityTable.idColumn,
                    metadataObjectType.name(),
                    deletedAt,
                    limit)
                + mapper.softDeleteOrphanedStatistics(
                    entityTable.tableName,
                    entityTable.idColumn,
                    metadataObjectType.name(),
                    deletedAt,
                    limit)
                + mapper.softDeleteOrphanedSecurableObjects(
                    entityTable.tableName,
                    entityTable.idColumn,
                    metadataObjectType.name(),
                    deletedAt,
                    limit));
  }

  private static class EntityTable {
    private final String tableName;
    private final String idColumn;

    private EntityTable(String tableName, String idColumn) {
      this.tableName = tableName;
      this.idColumn = idColumn;
    }
  }
}
