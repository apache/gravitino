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

import org.apache.gravitino.storage.relational.mapper.OntologyVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.OntologyVersionInfoPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for ontology version snapshots. */
public class OntologyVersionInfoBaseSQLProvider {

  /** Returns SQL for inserting an ontology version snapshot. */
  public String insertOntologyVersionInfo(
      @Param("ontologyVersionInfo") OntologyVersionInfoPO versionInfoPO) {
    return "INSERT INTO "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " (metalake_id, ontology_id, version, ontology_name, ontology_comment,"
        + " ai_context, requires_sql, ontology, prefixes, properties, audit_info, deleted_at)"
        + " VALUES (#{ontologyVersionInfo.metalakeId},"
        + " #{ontologyVersionInfo.ontologyId}, #{ontologyVersionInfo.version},"
        + " #{ontologyVersionInfo.ontologyName}, #{ontologyVersionInfo.ontologyComment},"
        + " #{ontologyVersionInfo.aiContext}, #{ontologyVersionInfo.requires},"
        + " #{ontologyVersionInfo.ontology}, #{ontologyVersionInfo.prefixes},"
        + " #{ontologyVersionInfo.properties}, #{ontologyVersionInfo.auditInfo},"
        + " #{ontologyVersionInfo.deletedAt})";
  }

  /** Returns SQL for selecting an ontology version snapshot. */
  public String selectOntologyVersionInfoByOntologyIdAndVersion(
      @Param("ontologyId") Long ontologyId, @Param("version") Integer version) {
    return "SELECT id as id, metalake_id as metalakeId, ontology_id as ontologyId,"
        + " version as version, ontology_name as ontologyName,"
        + " ontology_comment as ontologyComment, ai_context as aiContext,"
        + " requires_sql as requires, ontology as ontology, prefixes as prefixes,"
        + " properties as properties, audit_info as auditInfo, deleted_at as deletedAt FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE ontology_id = #{ontologyId}"
        + " AND version = #{version} AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting all snapshots for an ontology ID. */
  public String softDeleteOntologyVersionsByOntologyId(@Param("ontologyId") Long ontologyId) {
    return softDeleteBy("ontology_id = #{ontologyId}");
  }

  /** Returns SQL for soft-deleting ontology snapshots by metalake ID. */
  public String softDeleteOntologyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for permanently deleting old ontology snapshots. */
  public String deleteOntologyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns SQL for finding ontologies that exceed a version retention count. */
  public String selectOntologyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return "SELECT ontology_id as ontologyId, MAX(version) as version FROM "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " WHERE version > #{versionRetentionCount} AND deleted_at = 0"
        + " GROUP BY ontology_id";
  }

  /** Returns SQL for soft-deleting snapshots through a per-ontology retention line. */
  public String softDeleteOntologyVersionsByRetentionLine(
      @Param("ontologyId") Long ontologyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return "UPDATE "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE ontology_id = #{ontologyId}"
        + " AND version <= #{versionRetentionLine} AND deleted_at = 0 LIMIT #{limit}";
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + OntologyVersionInfoMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
