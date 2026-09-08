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
import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis mapper for Semantic Model version snapshot operations. */
public interface SemanticModelVersionInfoMapper {

  /** The Semantic Model version snapshot table name. */
  String TABLE_NAME = "semantic_model_version_info";

  /** Inserts a Semantic Model version snapshot. */
  @InsertProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "insertSemanticModelVersionInfo")
  void insertSemanticModelVersionInfo(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO);

  /** Inserts or overwrites a Semantic Model version snapshot. */
  @InsertProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "insertSemanticModelVersionInfoOnDuplicateKeyUpdate")
  void insertSemanticModelVersionInfoOnDuplicateKeyUpdate(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO);

  /** Selects a Semantic Model version snapshot. */
  @SelectProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "selectSemanticModelVersionInfoBySemanticModelIdAndVersion")
  SemanticModelVersionInfoPO selectSemanticModelVersionInfoBySemanticModelIdAndVersion(
      @Param("semanticModelId") Long semanticModelId, @Param("version") Integer version);

  /** Soft-deletes all snapshots for a Semantic Model ID. */
  @UpdateProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "softDeleteSemanticModelVersionsBySemanticModelId")
  Integer softDeleteSemanticModelVersionsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId);

  /** Soft-deletes Semantic Model snapshots under schemas. */
  @UpdateProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "softDeleteSemanticModelVersionsBySchemaIds")
  Integer softDeleteSemanticModelVersionsBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  /** Soft-deletes Semantic Model snapshots under a catalog. */
  @UpdateProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "softDeleteSemanticModelVersionsByCatalogId")
  Integer softDeleteSemanticModelVersionsByCatalogId(@Param("catalogId") Long catalogId);

  /** Soft-deletes Semantic Model snapshots under a metalake. */
  @UpdateProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "softDeleteSemanticModelVersionsByMetalakeId")
  Integer softDeleteSemanticModelVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Permanently deletes soft-deleted snapshots older than a timeline. */
  @DeleteProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "deleteSemanticModelVersionsByLegacyTimeline")
  Integer deleteSemanticModelVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  /** Selects active Semantic Models whose version count exceeds the retention count. */
  @SelectProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "selectSemanticModelVersionsByRetentionCount")
  List<SemanticModelVersionInfoPO> selectSemanticModelVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  /** Soft-deletes old snapshots through a per-model retention line. */
  @UpdateProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "softDeleteSemanticModelVersionsByRetentionLine")
  Integer softDeleteSemanticModelVersionsByRetentionLine(
      @Param("semanticModelId") Long semanticModelId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);
}
