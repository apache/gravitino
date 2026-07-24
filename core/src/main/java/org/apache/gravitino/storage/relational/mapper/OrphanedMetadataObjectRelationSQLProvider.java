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
import static org.apache.gravitino.storage.relational.mapper.PolicyMetadataObjectRelMapper.POLICY_METADATA_OBJECT_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper.STATISTIC_META_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME;

import com.google.common.base.Preconditions;
import org.apache.ibatis.annotations.Param;

/** Provides SQL for collecting orphaned metadata-object relations. */
public class OrphanedMetadataObjectRelationSQLProvider {

  /** Returns SQL that soft-deletes orphaned owner relations. */
  public static String softDeleteOrphanedOwnerRelations(
      @Param("entityTable") String entityTable, @Param("entityIdColumn") String entityIdColumn) {
    return softDeleteOrphans(OWNER_TABLE_NAME, "metadata_object_type", entityTable, entityIdColumn);
  }

  /** Returns SQL that soft-deletes orphaned tag relations. */
  public static String softDeleteOrphanedTagRelations(
      @Param("entityTable") String entityTable, @Param("entityIdColumn") String entityIdColumn) {
    return softDeleteOrphans(
        TAG_METADATA_OBJECT_RELATION_TABLE_NAME,
        "metadata_object_type",
        entityTable,
        entityIdColumn);
  }

  /** Returns SQL that soft-deletes orphaned policy relations. */
  public static String softDeleteOrphanedPolicyRelations(
      @Param("entityTable") String entityTable, @Param("entityIdColumn") String entityIdColumn) {
    return softDeleteOrphans(
        POLICY_METADATA_OBJECT_RELATION_TABLE_NAME,
        "metadata_object_type",
        entityTable,
        entityIdColumn);
  }

  /** Returns SQL that soft-deletes orphaned statistics. */
  public static String softDeleteOrphanedStatistics(
      @Param("entityTable") String entityTable, @Param("entityIdColumn") String entityIdColumn) {
    return softDeleteOrphans(
        STATISTIC_META_TABLE_NAME, "metadata_object_type", entityTable, entityIdColumn);
  }

  /** Returns SQL that soft-deletes orphaned securable objects. */
  public static String softDeleteOrphanedSecurableObjects(
      @Param("entityTable") String entityTable, @Param("entityIdColumn") String entityIdColumn) {
    return softDeleteOrphans(SECURABLE_OBJECT_TABLE_NAME, "type", entityTable, entityIdColumn);
  }

  private static String softDeleteOrphans(
      String relationTable, String typeColumn, String entityTable, String entityIdColumn) {
    Preconditions.checkArgument(
        entityTable.matches("[a-z_]+") && entityIdColumn.matches("[a-z_]+"),
        "Invalid entity table mapping: %s.%s",
        entityTable,
        entityIdColumn);
    return "UPDATE "
        + relationTable
        + " SET deleted_at = #{deletedAt}"
        + " WHERE deleted_at = 0 AND "
        + typeColumn
        + " = #{metadataObjectType} AND metadata_object_id IN ("
        + "SELECT metadata_object_id FROM (SELECT DISTINCT rel.metadata_object_id FROM "
        + relationTable
        + " rel WHERE rel.deleted_at = 0 AND rel."
        + typeColumn
        + " = #{metadataObjectType} AND NOT EXISTS (SELECT 1 FROM "
        + entityTable
        + " entity WHERE entity."
        + entityIdColumn
        + " = rel.metadata_object_id AND entity.deleted_at = 0)"
        + " LIMIT #{limit}) orphan_ids)";
  }
}
