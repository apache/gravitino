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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import org.apache.gravitino.storage.relational.mapper.OntologyVersionInfoMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.OntologyVersionInfoBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** Provides PostgreSQL SQL for ontology version snapshots. */
public class OntologyVersionInfoPostgreSQLProvider extends OntologyVersionInfoBaseSQLProvider {

  @Override
  public String softDeleteOntologyVersionsByOntologyId(@Param("ontologyId") Long ontologyId) {
    return softDeleteBy("ontology_id = #{ontologyId}");
  }

  @Override
  public String softDeleteOntologyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  @Override
  public String deleteOntologyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  public String softDeleteOntologyVersionsByRetentionLine(
      @Param("ontologyId") Long ontologyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE id IN (SELECT id FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE ontology_id = #{ontologyId}"
        + " AND version <= #{versionRetentionLine}"
        + " AND deleted_at = 0 LIMIT #{limit})";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
