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
import org.apache.gravitino.semantic.SemanticModelSource;
import org.apache.gravitino.storage.relational.po.SemanticModelSourceListingPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.UpdateProvider;

/** Reads and persists dataset-to-source associations and their retirement state. */
public interface SemanticModelSourceMapper {
  /** The dataset source association table name. */
  String TABLE_NAME = "semantic_model_source_rel";

  /** Inserts live associations with database-generated row IDs. */
  @Insert(
      """
      <script>INSERT INTO semantic_model_source_rel
      (metalake_id, semantic_model_id, dataset_name, source_type, source_id, deleted_at) VALUES
      <foreach collection='sources' item='source' separator=','>
      (#{metalakeId}, #{modelId}, #{source.datasetName}, #{source.sourceType}, #{source.sourceId}, 0)
      </foreach></script>
      """)
  void insertSources(
      @Param("metalakeId") long metalakeId,
      @Param("modelId") long modelId,
      @Param("sources") List<SemanticModelSource> sources);

  /** Retires all live associations of a model. */
  @UpdateProvider(
      type = SemanticModelSourceSQLProviderFactory.class,
      method = "softDeleteByModelId")
  int softDeleteByModelId(@Param("modelId") Long modelId);

  /** Retires live associations owned by models in the given schemas. */
  @UpdateProvider(
      type = SemanticModelSourceSQLProviderFactory.class,
      method = "softDeleteBySchemaIds")
  int softDeleteBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  /** Retires live associations owned by models in a catalog. */
  @UpdateProvider(
      type = SemanticModelSourceSQLProviderFactory.class,
      method = "softDeleteByCatalogId")
  int softDeleteByCatalogId(@Param("catalogId") Long catalogId);

  /** Retires live associations owned by models in a metalake. */
  @UpdateProvider(
      type = SemanticModelSourceSQLProviderFactory.class,
      method = "softDeleteByMetalakeId")
  int softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Permanently deletes retired associations older than a timeline. */
  @DeleteProvider(
      type = SemanticModelSourceSQLProviderFactory.class,
      method = "deleteByLegacyTimeline")
  int deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  /** Reads live models and matching datasets with current parent names in a single statement. */
  @Select(
      """
      SELECT model.semantic_model_id AS semanticModelId, model.semantic_model_name AS semanticModelName,
             metalake.metalake_name AS metalakeName, catalog.catalog_name AS catalogName,
             schema_meta.schema_name AS schemaName, rel.dataset_name AS datasetName
      FROM semantic_model_source_rel rel
      JOIN semantic_model_meta model ON model.semantic_model_id = rel.semantic_model_id AND model.deleted_at = 0
      JOIN schema_meta ON schema_meta.schema_id = model.schema_id AND schema_meta.deleted_at = 0
      JOIN catalog_meta catalog ON catalog.catalog_id = model.catalog_id AND catalog.deleted_at = 0
      JOIN metalake_meta metalake ON metalake.metalake_id = rel.metalake_id AND metalake.deleted_at = 0
      WHERE rel.deleted_at = 0 AND rel.metalake_id = #{metalakeId} AND rel.source_type = #{sourceType} AND rel.source_id = #{sourceId}
      """)
  List<SemanticModelSourceListingPO> listForSource(
      @Param("metalakeId") long metalakeId,
      @Param("sourceType") String sourceType,
      @Param("sourceId") long sourceId);
}
