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

import java.util.Map;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;

/** Portable SQL for glossary definitions and synonyms on H2, MySQL and PostgreSQL. */
public class GlossaryTermMetaBaseSQLProvider {
  private static final String LIVE_METALAKE_JOIN =
      " JOIN metalake_meta m ON m.metalake_id=t.metalake_id "
          + "AND m.metalake_name=#{metalakeName} AND m.deleted_at=0 ";

  /** The live term selected by the caller's metalake ID and normalized name. */
  protected static final String LIVE_NAME =
      "metalake_id=#{metalakeId} AND name_key=#{key} AND deleted_at=0";

  /** Locks a definition after the caller has fenced its metalake. */
  public String getForUpdate() {
    return selectForUpdate(" WHERE " + LIVE_NAME);
  }

  /** Resolves IDs only while the parent still has the requested live name. */
  public String getIds() {
    return "SELECT t.term_id AS termId, t.metalake_id AS metalakeId FROM glossary_term_meta t"
        + LIVE_METALAKE_JOIN
        + "WHERE t.metalake_id=#{metalakeId} AND t.name_key=#{key} AND t.deleted_at=0";
  }

  /** Returns SQL for insert. */
  public String insert() {
    return "INSERT INTO glossary_term_meta (term_id, name, name_key, metalake_id, definition, "
        + "status, links, source_ref, properties, audit_info, current_version, deleted_at) "
        + "VALUES (#{p.termId}, #{p.name}, #{p.nameKey}, #{p.metalakeId}, #{p.definition}, "
        + "#{p.status}, #{p.links}, #{p.sourceRef}, #{p.properties}, #{p.auditInfo}, "
        + "#{p.currentVersion}, #{p.deletedAt})";
  }
  /** Returns SQL for update. */
  public String update() {
    return "UPDATE glossary_term_meta SET name=#{p.name}, name_key=#{p.nameKey}, "
        + "definition=#{p.definition}, status=#{p.status}, links=#{p.links}, "
        + "source_ref=#{p.sourceRef}, properties=#{p.properties}, audit_info=#{p.auditInfo}, "
        + "current_version=#{p.currentVersion} WHERE term_id=#{p.termId} AND "
        + "metalake_id=#{p.metalakeId} AND current_version=#{version} AND deleted_at=0";
  }
  /** Returns SQL for synonyms. */
  public String synonyms() {
    return "SELECT synonym FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at=0 ORDER "
        + "BY synonym_key";
  }
  /** Returns SQL for addSynonym. */
  public String addSynonym() {
    return "INSERT INTO glossary_term_synonym (term_id, metalake_id, synonym, synonym_key, "
        + "deleted_at) VALUES (#{id},#{metalakeId},#{synonym},#{key},0)";
  }
  /** Returns SQL for clearSynonyms. */
  public String clearSynonyms() {
    return "DELETE FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at=0";
  }
  /** Returns SQL for delete. */
  public String delete() {
    return "UPDATE glossary_term_meta SET deleted_at=#{deletedAt} WHERE term_id=#{id} AND "
        + "current_version=#{version} AND deleted_at=0";
  }
  /** Returns SQL for deleteSynonyms. */
  public String deleteSynonyms() {
    return "UPDATE glossary_term_synonym SET deleted_at=#{deletedAt} WHERE term_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for dropMetalake. */
  public String dropMetalake() {
    return "UPDATE glossary_term_meta SET deleted_at=#{deletedAt} WHERE metalake_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for dropMetalakeSynonyms. */
  public String dropMetalakeSynonyms() {
    return "UPDATE glossary_term_synonym SET deleted_at=#{deletedAt} WHERE metalake_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for expired. */
  public String expired() {
    return "SELECT term_id FROM glossary_term_meta WHERE deleted_at>0 AND deleted_at<#{cutoff} "
        + "ORDER BY term_id LIMIT #{limit}";
  }
  /** Returns SQL for purgeSynonyms. */
  public String purgeSynonyms() {
    return "DELETE FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at>0";
  }
  /** Returns SQL for purge. */
  public String purge() {
    return "DELETE FROM glossary_term_meta WHERE term_id=#{id} AND deleted_at>0";
  }
  /** Reads definitions and synonyms together, including definitions without synonyms. */
  public String snapshot(Map<String, Object> params) {
    // Validate the parent name in this snapshot too: it can be renamed after ID resolution.
    // A single statement avoids mixed revisions at READ_COMMITTED without locking readers.
    return "<script>SELECT t.term_id AS termId, t.name AS name, t.name_key AS nameKey, "
        + "m.metalake_id AS metalakeId, t.definition AS definition, t.status AS status, "
        + "t.links AS links, t.source_ref AS sourceRef, t.properties AS properties, "
        + "t.audit_info AS auditInfo, t.current_version AS currentVersion, "
        + "t.deleted_at AS deletedAt, s.synonym AS synonym FROM metalake_meta m "
        + "LEFT JOIN glossary_term_meta t ON t.metalake_id=m.metalake_id AND t.deleted_at=0 "
        + (params.containsKey("key") && params.get("key") != null ? "AND t.name_key=#{key} " : "")
        + (params.containsKey("ids")
            ? "AND t.term_id IN <foreach item='id' collection='ids' open='(' close=')' separator=','>#{id}</foreach> "
            : "")
        + "LEFT JOIN glossary_term_synonym s ON s.term_id=t.term_id "
        + "AND s.metalake_id=t.metalake_id AND s.deleted_at=0 "
        + "WHERE m.metalake_id=#{metalakeId} AND m.metalake_name=#{metalakeName} AND m.deleted_at=0"
        + " ORDER BY t.term_id,s.synonym_key</script>";
  }

  /** Returns the SQL for {@link GlossaryTermMetaMapper#conflictingName}. */
  public String conflictingName() {
    return "<script>SELECT term_id FROM glossary_term_meta WHERE metalake_id=#{metalakeId} "
        + "AND term_id != #{id} AND deleted_at=0 AND name_key IN "
        + "<foreach collection='keys' item='key' open='(' separator=',' close=')'>#{key}</foreach> "
        + "LIMIT 1</script>";
  }

  /** Returns the SQL for {@link GlossaryTermMetaMapper#conflictingSynonym}. */
  public String conflictingSynonym() {
    return "SELECT term_id FROM glossary_term_synonym WHERE metalake_id=#{metalakeId} AND synonym_key=#{key} AND term_id<>#{id} AND deleted_at=0 LIMIT 1";
  }

  /** Returns the SQL for {@link GlossaryTermMetaMapper#idsByKeys}. */
  public String idsByKeys() {
    return "<script>SELECT name_key AS nameKey, term_id AS termId FROM glossary_term_meta "
        + "WHERE metalake_id=#{metalakeId} AND deleted_at=0 AND name_key IN "
        + "<foreach collection='keys' item='key' open='(' separator=',' close=')'>#{key}</foreach></script>";
  }

  /** Builds the locking read with the backend's access path. */
  protected String selectForUpdate(String lookup) {
    return "SELECT term_id AS termId, name AS name, name_key AS nameKey, metalake_id AS "
        + "metalakeId, definition AS definition, status AS status, links AS links, source_ref AS "
        + "sourceRef, properties AS properties, audit_info AS auditInfo, current_version AS "
        + "currentVersion, deleted_at AS deletedAt FROM glossary_term_meta"
        + lookup
        + " FOR UPDATE";
  }
}
