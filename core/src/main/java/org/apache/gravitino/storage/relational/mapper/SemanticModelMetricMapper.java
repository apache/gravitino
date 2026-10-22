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
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis mapper for identities of metrics contained in Semantic Models. */
public interface SemanticModelMetricMapper {
  /** The metric identity table name. */
  String TABLE_NAME = "semantic_model_metric_meta";

  /** Inserts a new metric identity. */
  @InsertProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "insertSemanticModelMetric")
  void insertSemanticModelMetric(@Param("metric") SemanticModelMetricPO metricPO);

  /** Locks and reads the live identities of a Semantic Model. */
  @SelectProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "selectSemanticModelMetricsByModelIdForUpdate")
  List<SemanticModelMetricPO> selectSemanticModelMetricsByModelIdForUpdate(
      @Param("semanticModelId") Long semanticModelId);

  /** Retires selected live identities belonging to a Semantic Model. */
  @UpdateProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "softDeleteSemanticModelMetricsByIds")
  Integer softDeleteSemanticModelMetricsByIds(
      @Param("semanticModelId") Long semanticModelId, @Param("metricIds") List<Long> metricIds);

  /** Retires all live metric identities of a Semantic Model. */
  @UpdateProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "softDeleteSemanticModelMetricsBySemanticModelId")
  Integer softDeleteSemanticModelMetricsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId);

  /** Retires live metric identities under schemas. */
  @UpdateProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "softDeleteSemanticModelMetricsBySchemaIds")
  Integer softDeleteSemanticModelMetricsBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  /** Retires live metric identities under a catalog. */
  @UpdateProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "softDeleteSemanticModelMetricsByCatalogId")
  Integer softDeleteSemanticModelMetricsByCatalogId(@Param("catalogId") Long catalogId);

  /** Retires live metric identities under a metalake. */
  @UpdateProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "softDeleteSemanticModelMetricsByMetalakeId")
  Integer softDeleteSemanticModelMetricsByMetalakeId(@Param("metalakeId") Long metalakeId);

  /** Permanently deletes retired identities older than a timeline. */
  @DeleteProvider(
      type = SemanticModelMetricSQLProviderFactory.class,
      method = "deleteSemanticModelMetricsByLegacyTimeline")
  Integer deleteSemanticModelMetricsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
