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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import static org.apache.gravitino.storage.relational.mapper.SemanticModelMetricMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.SemanticModelMetricPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for Semantic Model metric identities. */
public class SemanticModelMetricBaseSQLProvider {

  /** Returns SQL for inserting a new identity. */
  public String insertSemanticModelMetric(@Param("metric") SemanticModelMetricPO metricPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (metric_id, metalake_id, catalog_id, schema_id, semantic_model_id, metric_name, deleted_at)"
        + " VALUES (#{metric.metricId}, #{metric.metalakeId}, #{metric.catalogId},"
        + " #{metric.schemaId}, #{metric.semanticModelId}, #{metric.metricName}, #{metric.deletedAt})";
  }

  /** Returns a locking read of live identities after the containing model's concurrency check. */
  public String selectSemanticModelMetricsByModelIdForUpdate(
      @Param("semanticModelId") Long semanticModelId) {
    return "SELECT metric_id as metricId, metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId, semantic_model_id as semanticModelId, metric_name as metricName,"
        + " deleted_at as deletedAt FROM "
        + TABLE_NAME
        + " WHERE semantic_model_id = #{semanticModelId} AND deleted_at = 0 FOR UPDATE";
  }

  /** Returns SQL for retiring selected live identities of a model. */
  public String softDeleteSemanticModelMetricsByIds(
      @Param("semanticModelId") Long semanticModelId, @Param("metricIds") List<Long> metricIds) {
    return "<script>"
        + softDeleteBy(
            "semantic_model_id = #{semanticModelId} AND metric_id IN ("
                + "<foreach collection='metricIds' item='metricId' separator=','>"
                + "#{metricId}</foreach>)")
        + "</script>";
  }

  /** Returns SQL for retiring all live identities of a model. */
  public String softDeleteSemanticModelMetricsBySemanticModelId(
      @Param("semanticModelId") Long semanticModelId) {
    return softDeleteBy("semantic_model_id = #{semanticModelId}");
  }

  /** Returns SQL for retiring live identities under schemas. */
  public String softDeleteSemanticModelMetricsBySchemaIds(
      @Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + softDeleteBy(
            "schema_id IN ("
                + "<foreach collection='schemaIds' item='schemaId' separator=','>"
                + "#{schemaId}</foreach>)")
        + "</script>";
  }

  /** Returns SQL for retiring live identities under a catalog. */
  public String softDeleteSemanticModelMetricsByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy("catalog_id = #{catalogId}");
  }

  /** Returns SQL for retiring live identities under a metalake. */
  public String softDeleteSemanticModelMetricsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for permanently deleting old retired identities. */
  public String deleteSemanticModelMetricsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns the database expression for the current time in epoch milliseconds. */
  protected String currentTimestampMillis() {
    return "(UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + currentTimestampMillis()
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
