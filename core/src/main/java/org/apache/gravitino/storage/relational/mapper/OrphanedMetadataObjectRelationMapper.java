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

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis mapper for collecting orphaned metadata-object relations. */
public interface OrphanedMetadataObjectRelationMapper {

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

  /** Soft-deletes orphaned policy relations and returns the affected row count. */
  @UpdateProvider(
      type = OrphanedMetadataObjectRelationSQLProvider.class,
      method = "softDeleteOrphanedPolicyRelations")
  int softDeleteOrphanedPolicyRelations(
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
