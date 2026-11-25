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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.DomainMemberMapper;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for glossary definitions and synonyms on H2, MySQL and PostgreSQL. */
public class GlossaryTermMetaBaseSQLProvider {
  /** Selects a term definition for a locking read. */
  protected static final String SELECT_TERM =
      "SELECT term_id AS termId, name AS name, name_key AS nameKey, metalake_id AS "
          + "metalakeId, definition AS definition, status AS status, links AS links, source_ref AS "
          + "sourceRef, properties AS properties, audit_info AS auditInfo, current_version AS "
          + "currentVersion, deleted_at AS deletedAt FROM "
          + GlossaryTermMetaMapper.TABLE_NAME;

  private static final String SNAPSHOT_SELECT =
      "<script>SELECT t.term_id AS termId, t.name AS name, t.name_key AS nameKey, "
          + "m.metalake_id AS metalakeId, t.definition AS definition, t.status AS status, "
          + "t.links AS links, t.source_ref AS sourceRef, t.properties AS properties, "
          + "t.audit_info AS auditInfo, t.current_version AS currentVersion, "
          + "t.deleted_at AS deletedAt, s.synonym AS synonym, d.domain_name AS domainName FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " m LEFT JOIN "
          + GlossaryTermMetaMapper.TABLE_NAME
          + " t ON t.metalake_id=m.metalake_id AND t.deleted_at=0 ";

  // The two branches return synonyms and memberships separately. A term with S synonyms and D
  // domains contributes max(1, S) + D rows in one statement, including at READ_COMMITTED.
  private static final String SNAPSHOT_DETAILS =
      "CROSS JOIN (SELECT 0 AS domain_row UNION ALL SELECT 1) kind "
          + "LEFT JOIN "
          + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
          + " s ON kind.domain_row=0 AND s.term_id=t.term_id "
          + "AND s.metalake_id=t.metalake_id AND s.deleted_at=0 "
          + "LEFT JOIN "
          + DomainMemberMapper.TABLE_NAME
          + " r ON kind.domain_row=1 AND r.member_type='GLOSSARY_TERM' AND r.member_id=t.term_id "
          + "AND r.metalake_id=t.metalake_id AND r.deleted_at=0 "
          + "LEFT JOIN "
          + DomainMetaMapper.DOMAIN_TABLE_NAME
          + " d ON d.domain_id=r.domain_id AND d.metalake_id=r.metalake_id AND d.deleted_at=0 "
          + "WHERE m.metalake_id=#{metalakeId} AND m.metalake_name=#{metalakeName} AND m.deleted_at=0 "
          + "AND (kind.domain_row=0 OR d.domain_id IS NOT NULL) "
          + "ORDER BY t.term_id,kind.domain_row,s.synonym_key,d.domain_name</script>";

  /** Locks a definition after the caller has fenced its metalake. */
  public String getForUpdate(@Param("metalakeId") long metalakeId, @Param("key") String key) {
    return SELECT_TERM + " WHERE " + liveTermByName(metalakeId, key) + " FOR UPDATE";
  }

  /** Locks a live definition by ID in its metalake. */
  public String getByIdForUpdate(
      @Param("metalakeId") long metalakeId, @Param("termId") long termId) {
    return SELECT_TERM
        + " WHERE metalake_id=#{metalakeId} AND term_id=#{termId} AND deleted_at=0 FOR UPDATE";
  }

  /** Increments the revision of a live term by one when its version matches. */
  public String incrementVersion(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("version") long version,
      @Param("auditInfo") String auditInfo) {
    return "UPDATE "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " SET current_version = current_version + 1, audit_info=#{auditInfo}"
        + " WHERE term_id=#{termId} AND metalake_id=#{metalakeId}"
        + " AND current_version=#{version} AND deleted_at=0";
  }

  /** Resolves IDs only while the parent still has the requested live name. */
  public String getIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") String key) {
    return "SELECT t.term_id AS termId, t.metalake_id AS metalakeId FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " t"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " m ON m.metalake_id=t.metalake_id "
        + "AND m.metalake_name=#{metalakeName} AND m.deleted_at=0 "
        + "WHERE t.metalake_id=#{metalakeId} AND t.name_key=#{key} AND t.deleted_at=0";
  }

  /** Returns SQL for insert. */
  public String insert(@Param("term") GlossaryTermPO term) {
    return "INSERT INTO "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " (term_id, name, name_key, metalake_id, definition, "
        + "status, links, source_ref, properties, audit_info, current_version, deleted_at) "
        + "VALUES (#{term.termId}, #{term.name}, #{term.nameKey}, #{term.metalakeId}, #{term.definition}, "
        + "#{term.status}, #{term.links}, #{term.sourceRef}, #{term.properties}, #{term.auditInfo}, "
        + "#{term.currentVersion}, #{term.deletedAt})";
  }

  /** Returns SQL for update. */
  public String update(@Param("term") GlossaryTermPO term, @Param("version") long version) {
    return "UPDATE "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " SET name=#{term.name}, name_key=#{term.nameKey}, "
        + "definition=#{term.definition}, status=#{term.status}, links=#{term.links}, "
        + "source_ref=#{term.sourceRef}, properties=#{term.properties}, audit_info=#{term.auditInfo}, "
        + "current_version=#{term.currentVersion} WHERE term_id=#{term.termId} AND "
        + "metalake_id=#{term.metalakeId} AND current_version=#{version} AND deleted_at=0";
  }

  /** Returns SQL for synonyms. */
  public String synonyms(@Param("termId") long termId) {
    return "SELECT synonym FROM "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " WHERE term_id=#{termId} AND deleted_at=0 ORDER "
        + "BY synonym_key";
  }

  /** Returns SQL for addSynonym. */
  public String addSynonym(
      @Param("termId") long termId,
      @Param("metalakeId") long metalakeId,
      @Param("synonym") String synonym,
      @Param("key") String key) {
    return "INSERT INTO "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " (term_id, metalake_id, synonym, synonym_key, "
        + "deleted_at) VALUES (#{termId},#{metalakeId},#{synonym},#{key},0)";
  }

  /** Returns SQL for clearSynonyms. */
  public String clearSynonyms(@Param("termId") long termId) {
    return "DELETE FROM "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " WHERE term_id=#{termId} AND deleted_at=0";
  }

  /** Returns SQL for delete. */
  public String delete(
      @Param("termId") long termId,
      @Param("version") long version,
      @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE term_id=#{termId} AND "
        + "current_version=#{version} AND deleted_at=0";
  }

  /** Returns SQL for deleteSynonyms. */
  public String deleteSynonyms(@Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE term_id=#{termId} AND "
        + "deleted_at=0";
  }

  /** Returns SQL for dropMetalake. */
  public String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND "
        + "deleted_at=0";
  }

  /** Returns SQL for dropMetalakeSynonyms. */
  public String dropMetalakeSynonyms(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND "
        + "deleted_at=0";
  }

  /** Returns SQL for expired. */
  public String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return "SELECT term_id FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " WHERE deleted_at>0 AND deleted_at<#{cutoff} "
        + "ORDER BY term_id LIMIT #{limit}";
  }

  /** Returns SQL for purgeSynonyms. */
  public String purgeSynonyms(@Param("termId") long termId) {
    return "DELETE FROM "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " WHERE term_id=#{termId} AND deleted_at>0";
  }

  /** Returns SQL for purge. */
  public String purge(@Param("termId") long termId) {
    return "DELETE FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " WHERE term_id=#{termId} AND deleted_at>0";
  }

  /** Reads all live definitions in the metalake, or just the term with the supplied key. */
  public String snapshot(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("key") @Nullable String key) {
    return SNAPSHOT_SELECT + (key == null ? "" : "AND t.name_key=#{key} ") + SNAPSHOT_DETAILS;
  }

  /** Reads a bounded batch of live terms by ID. */
  public String selectTermSnapshotsByIds(
      @Param("metalakeId") long metalakeId,
      @Param("metalakeName") String metalakeName,
      @Param("ids") List<Long> ids) {
    return SNAPSHOT_SELECT
        + "AND t.term_id IN <foreach item='id' collection='ids' open='(' close=')' separator=','>#{id}</foreach> "
        + SNAPSHOT_DETAILS;
  }

  /** Finds a different live term whose name matches one of the proposed synonym keys. */
  public String conflictingName(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("keys") List<String> keys) {
    return "<script>SELECT term_id FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " WHERE metalake_id=#{metalakeId} "
        + "AND term_id != #{excludedTermId} AND deleted_at=0 AND name_key IN "
        + "<foreach collection='keys' item='key' open='(' separator=',' close=')'>#{key}</foreach> "
        + "LIMIT 1</script>";
  }

  /** Finds a different live term whose synonym matches the proposed term name. */
  public String conflictingSynonym(
      @Param("metalakeId") long metalakeId,
      @Param("excludedTermId") long excludedTermId,
      @Param("key") String key) {
    return "SELECT term_id FROM "
        + GlossaryTermMetaMapper.SYNONYM_TABLE_NAME
        + " WHERE metalake_id=#{metalakeId}"
        + " AND synonym_key=#{key} AND term_id<>#{excludedTermId} AND deleted_at=0 LIMIT 1";
  }

  /** Resolves normalized term names to the IDs of their live definitions. */
  public String idsByKeys(@Param("metalakeId") long metalakeId, @Param("keys") List<String> keys) {
    return "<script>SELECT name_key AS nameKey, term_id AS termId FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " "
        + "WHERE metalake_id=#{metalakeId} AND deleted_at=0 AND name_key IN "
        + "<foreach collection='keys' item='key' open='(' separator=',' close=')'>#{key}</foreach></script>";
  }

  /** Matches the live term under its metalake ID and normalized name. */
  protected String liveTermByName(long metalakeId, String key) {
    return "metalake_id=#{metalakeId} AND name_key=#{key} AND deleted_at=0";
  }
}
