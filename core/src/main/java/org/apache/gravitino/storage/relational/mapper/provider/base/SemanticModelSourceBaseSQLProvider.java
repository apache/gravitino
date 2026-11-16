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

import static org.apache.gravitino.storage.relational.mapper.SemanticModelSourceMapper.TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for retiring dataset source associations. */
public class SemanticModelSourceBaseSQLProvider {

  /** Returns SQL for retiring a model's live associations. */
  public String softDeleteByModelId(@Param("modelId") Long modelId) {
    return softDeleteBy("semantic_model_id = #{modelId}");
  }

  /** Returns SQL for retiring associations owned by models in the given schemas. */
  public String softDeleteBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + softDeleteBy(
            "semantic_model_id IN (SELECT semantic_model_id FROM "
                + SemanticModelMetaMapper.TABLE_NAME
                + " WHERE schema_id IN ("
                + "<foreach collection='schemaIds' item='schemaId' separator=','>"
                + "#{schemaId}</foreach>))")
        + "</script>";
  }

  /** Returns SQL for retiring associations owned by models in a catalog. */
  public String softDeleteByCatalogId(@Param("catalogId") Long catalogId) {
    return softDeleteBy(
        "semantic_model_id IN (SELECT semantic_model_id FROM "
            + SemanticModelMetaMapper.TABLE_NAME
            + " WHERE catalog_id = #{catalogId})");
  }

  /** Returns SQL for retiring associations owned by models in a metalake. */
  public String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for purging retired rows without depending on model or parent metadata. */
  public String deleteByLegacyTimeline(
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
