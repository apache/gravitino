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

import static org.apache.gravitino.storage.relational.mapper.OntologyMetaMapper.TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.OntologyMetaMapper.VERSION_TABLE_NAME;

import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.OntologyPO;
import org.apache.ibatis.annotations.Param;

/** Provides MySQL-compatible SQL for ontology identity metadata. */
public class OntologyMetaBaseSQLProvider {

  private static final String CURRENT_SNAPSHOT_COLUMNS =
      " oom.ontology_id, oom.ontology_name, oom.metalake_id, oom.status,"
          + " oom.current_version, oom.last_version, oom.audit_info, oom.deleted_at, ovi.id,"
          + " ovi.metalake_id as version_metalake_id,"
          + " ovi.ontology_id as version_ontology_id, ovi.version,"
          + " ovi.ontology_name as version_ontology_name, ovi.ontology_comment, ovi.ai_context,"
          + " ovi.requires_sql, ovi.ontology, ovi.prefixes, ovi.properties,"
          + " ovi.audit_info as version_audit_info,"
          + " ovi.deleted_at as version_deleted_at";

  private static final String CURRENT_SNAPSHOT_JOIN =
      " FROM "
          + TABLE_NAME
          + " oom INNER JOIN "
          + VERSION_TABLE_NAME
          + " ovi ON oom.ontology_id = ovi.ontology_id"
          + " AND oom.current_version = ovi.version";

  /** Returns SQL for listing current ontology snapshots by metalake ID. */
  public String listOntologyPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + CURRENT_SNAPSHOT_JOIN
        + " WHERE oom.metalake_id = #{metalakeId}"
        + " AND oom.deleted_at = 0 AND ovi.deleted_at = 0";
  }

  /** Returns SQL for listing current ontology snapshots by metalake name. */
  public String listOntologyPOsByFullQualifiedName(@Param("metalakeName") String metalakeName) {
    return qualifiedNameSelect(null);
  }

  /** Returns SQL for selecting an active ontology ID by metalake ID and name. */
  public String selectOntologyIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return "SELECT ontology_id as ontologyId FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId}"
        + " AND ontology_name = #{ontologyName} AND deleted_at = 0";
  }

  /** Returns SQL for selecting a current ontology snapshot by metalake ID and name. */
  public String selectOntologyMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + CURRENT_SNAPSHOT_JOIN
        + " WHERE oom.metalake_id = #{metalakeId}"
        + " AND oom.ontology_name = #{ontologyName}"
        + " AND oom.deleted_at = 0 AND ovi.deleted_at = 0";
  }

  /** Returns SQL for selecting and locking an active ontology identity by natural key. */
  public String selectOntologyMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("ontologyName") String ontologyName) {
    return identityColumns()
        + " FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId}"
        + " AND ontology_name = #{ontologyName}"
        + " AND deleted_at = 0 FOR UPDATE";
  }

  /** Returns SQL for selecting a current ontology snapshot by stable ID. */
  public String selectOntologyMetaById(@Param("ontologyId") Long ontologyId) {
    return "SELECT"
        + CURRENT_SNAPSHOT_COLUMNS
        + CURRENT_SNAPSHOT_JOIN
        + " WHERE oom.ontology_id = #{ontologyId}"
        + " AND oom.deleted_at = 0 AND ovi.deleted_at = 0";
  }

  /** Returns SQL for selecting and locking an ontology identity by stable ID. */
  public String selectOntologyMetaByIdForUpdate(@Param("ontologyId") Long ontologyId) {
    return identityColumns()
        + " FROM "
        + TABLE_NAME
        + " WHERE ontology_id = #{ontologyId} AND deleted_at = 0 FOR UPDATE";
  }

  /** Returns SQL for inserting an ontology identity row. */
  public String insertOntologyMeta(@Param("ontologyMeta") OntologyPO ontologyPO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (ontology_id, ontology_name, metalake_id, status,"
        + " current_version, last_version, audit_info, deleted_at)"
        + " VALUES (#{ontologyMeta.ontologyId},"
        + " #{ontologyMeta.ontologyName}, #{ontologyMeta.metalakeId},"
        + " #{ontologyMeta.status},"
        + " #{ontologyMeta.currentVersion}, #{ontologyMeta.lastVersion},"
        + " #{ontologyMeta.auditInfo}, #{ontologyMeta.deletedAt})";
  }

  /** Returns SQL for updating an ontology identity with a version check. */
  public String updateOntologyMeta(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET ontology_name = #{newOntologyMeta.ontologyName},"
        + " status = #{newOntologyMeta.status},"
        + " current_version = #{newOntologyMeta.currentVersion},"
        + " last_version = #{newOntologyMeta.lastVersion},"
        + " audit_info = #{newOntologyMeta.auditInfo},"
        + " deleted_at = #{newOntologyMeta.deletedAt}"
        + " WHERE ontology_id = #{oldOntologyMeta.ontologyId}"
        + " AND current_version = #{oldOntologyMeta.currentVersion}"
        + " AND deleted_at = 0"
        + " AND NOT EXISTS (SELECT 1 FROM "
        + VERSION_TABLE_NAME
        + " ovi WHERE ovi.ontology_id = #{oldOntologyMeta.ontologyId}"
        + " AND ovi.version >= #{newOntologyMeta.currentVersion}"
        + " AND ovi.deleted_at = 0)";
  }

  /**
   * Returns SQL that flips only the ontology status.
   *
   * <p>The statement keeps {@code current_version} and {@code last_version} untouched, so a status
   * change neither allocates nor hides a version snapshot.
   */
  public String updateOntologyMetaStatus(
      @Param("newOntologyMeta") OntologyPO newOntologyPO,
      @Param("oldOntologyMeta") OntologyPO oldOntologyPO) {
    return "UPDATE "
        + TABLE_NAME
        + " SET status = #{newOntologyMeta.status},"
        + " audit_info = #{newOntologyMeta.auditInfo}"
        + " WHERE ontology_id = #{oldOntologyMeta.ontologyId}"
        + " AND current_version = #{oldOntologyMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting an ontology identity with an optimistic version check. */
  public String softDeleteOntologyMetasByOntologyId(
      @Param("ontologyId") Long ontologyId, @Param("currentVersion") Integer currentVersion) {
    return softDeleteBy("ontology_id = #{ontologyId} AND current_version = #{currentVersion}");
  }

  /** Returns SQL for soft-deleting ontology identities by metalake ID. */
  public String softDeleteOntologyMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return softDeleteBy("metalake_id = #{metalakeId}");
  }

  /** Returns SQL for permanently deleting old ontology identity rows. */
  public String deleteOntologyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns SQL for selecting a current ontology by metalake and ontology name. */
  public String selectOntologyByFullQualifiedName(
      @Param("metalakeName") String metalakeName, @Param("ontologyName") String ontologyName) {
    return qualifiedNameSelect(ontologyName);
  }

  private static String identityColumns() {
    return "SELECT ontology_id as ontologyId, ontology_name as ontologyName,"
        + " metalake_id as metalakeId, status as status,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " audit_info as auditInfo, deleted_at as deletedAt";
  }

  private static String qualifiedNameSelect(@Nullable String ontologyName) {
    String ontologyNameCondition =
        ontologyName == null ? "" : " AND oom.ontology_name = #{ontologyName}";
    return """
        SELECT
            mm.metalake_id,
            oom.ontology_id,
            oom.ontology_name,
            oom.status,
            oom.current_version,
            oom.last_version,
            oom.audit_info,
            oom.deleted_at,
            ovi.id,
            ovi.metalake_id as version_metalake_id,
            ovi.ontology_id as version_ontology_id,
            ovi.version,
            ovi.ontology_name as version_ontology_name,
            ovi.ontology_comment,
            ovi.ai_context,
            ovi.requires_sql,
            ovi.ontology,
            ovi.prefixes,
            ovi.properties,
            ovi.audit_info as version_audit_info,
            ovi.deleted_at as version_deleted_at
        FROM
            %s mm
        LEFT JOIN
            %s oom ON oom.metalake_id = mm.metalake_id%s
            AND oom.deleted_at = 0
        LEFT JOIN
            %s ovi ON oom.ontology_id = ovi.ontology_id
            AND oom.current_version = ovi.version
            AND ovi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0
        """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME, TABLE_NAME, ontologyNameCondition, VERSION_TABLE_NAME);
  }

  private String softDeleteBy(String condition) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE "
        + condition
        + " AND deleted_at = 0";
  }
}
