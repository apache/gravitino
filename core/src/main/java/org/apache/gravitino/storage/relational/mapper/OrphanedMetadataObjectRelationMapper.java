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

import java.util.List;
import org.apache.ibatis.annotations.Arg;
import org.apache.ibatis.annotations.ConstructorArgs;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis mapper for collecting orphaned metadata-object relations. */
public interface OrphanedMetadataObjectRelationMapper {

  /** Locks a bounded batch of live Article binding row IDs whose registered targets are gone. */
  @SelectProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "orphanedArticleBindingIds")
  List<Long> orphanedArticleBindingIds(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("limit") int limit);

  /** Rechecks each candidate row and its target before retiring the binding. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "retireOrphanedArticleBindings")
  int retireOrphanedArticleBindings(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("ids") List<Long> ids,
      @Param("deletedAt") long deletedAt);

  /** One retired target and the number of bindings removed in the current batch. */
  record RetiredTarget(long metalakeId, long targetId, int removedRows) {}

  /** Summarizes the locked rows retired by this sweep, excluding candidates that became live. */
  @Select({
    "<script>SELECT metalake_id, target_id, COUNT(*) AS removed_rows FROM kb_article_relation_meta",
    "WHERE deleted_at=#{deletedAt} AND id IN",
    "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>",
    "GROUP BY metalake_id, target_id ORDER BY metalake_id, target_id</script>"
  })
  @ConstructorArgs({
    @Arg(column = "metalake_id", javaType = long.class),
    @Arg(column = "target_id", javaType = long.class),
    @Arg(column = "removed_rows", javaType = int.class)
  })
  List<RetiredTarget> listRetiredArticleTargets(
      @Param("ids") List<Long> ids, @Param("deletedAt") long deletedAt);

  /** Soft-deletes orphaned owner relations and returns the affected row count. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "softDeleteOrphanedOwnerRelations")
  int softDeleteOrphanedOwnerRelations(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("deletedAt") long deletedAt,
      @Param("limit") int limit);

  /** Soft-deletes orphaned tag relations and returns the affected row count. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "softDeleteOrphanedTagRelations")
  int softDeleteOrphanedTagRelations(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("deletedAt") long deletedAt,
      @Param("limit") int limit);

  /** Soft-deletes orphaned statistics and returns the affected row count. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "softDeleteOrphanedStatistics")
  int softDeleteOrphanedStatistics(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("deletedAt") long deletedAt,
      @Param("limit") int limit);

  /** Soft-deletes orphaned securable objects and returns the affected row count. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "softDeleteOrphanedSecurableObjects")
  int softDeleteOrphanedSecurableObjects(
      @Param("entityTable") String entityTable,
      @Param("entityIdColumn") String entityIdColumn,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("deletedAt") long deletedAt,
      @Param("limit") int limit);
}
