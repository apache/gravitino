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
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** Portable SQL for glossary definitions and synonyms on H2, MySQL and PostgreSQL. */
public final class GlossaryTermSQLProvider {
  /** Returns SQL for get. */
  public static String get(Map<String, Object> params) {
    return "SELECT term_id AS termId, name AS name, name_key AS nameKey, metalake_id AS "
        + "metalakeId, definition AS definition, status AS status, links AS links, source_ref AS "
        + "sourceRef, properties AS properties, audit_info AS auditInfo, current_version AS "
        + "currentVersion, deleted_at AS deletedAt FROM glossary_term_meta WHERE metalake_id = "
        + "#{metalakeId} AND name_key = #{key} AND deleted_at=0"
        + lock(Boolean.TRUE.equals(params.get("write")));
  }
  /** Returns SQL for list. */
  public static String list() {
    return "SELECT term_id AS termId, name AS name, name_key AS nameKey, metalake_id AS "
        + "metalakeId, definition AS definition, status AS status, links AS links, source_ref AS "
        + "sourceRef, properties AS properties, audit_info AS auditInfo, current_version AS "
        + "currentVersion, deleted_at AS deletedAt FROM glossary_term_meta WHERE metalake_id = "
        + "#{metalakeId} AND deleted_at=0 ORDER BY term_id"
        + lock(false);
  }
  /** Returns SQL for insert. */
  public static String insert() {
    return "INSERT INTO glossary_term_meta (term_id, name, name_key, metalake_id, definition, "
        + "status, links, source_ref, properties, audit_info, current_version, deleted_at) "
        + "VALUES (#{p.termId}, #{p.name}, #{p.nameKey}, #{p.metalakeId}, #{p.definition}, "
        + "#{p.status}, #{p.links}, #{p.sourceRef}, #{p.properties}, #{p.auditInfo}, "
        + "#{p.currentVersion}, #{p.deletedAt})";
  }
  /** Returns SQL for update. */
  public static String update() {
    return "UPDATE glossary_term_meta SET name=#{p.name}, name_key=#{p.nameKey}, "
        + "definition=#{p.definition}, status=#{p.status}, links=#{p.links}, "
        + "source_ref=#{p.sourceRef}, properties=#{p.properties}, audit_info=#{p.auditInfo}, "
        + "current_version=#{p.currentVersion} WHERE term_id=#{p.termId} AND "
        + "metalake_id=#{p.metalakeId} AND current_version=#{version} AND deleted_at=0";
  }
  /** Returns SQL for synonyms. */
  public static String synonyms() {
    return "SELECT synonym FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at=0 ORDER "
        + "BY synonym_key";
  }
  /** Returns SQL for addSynonym. */
  public static String addSynonym() {
    return "INSERT INTO glossary_term_synonym (term_id, metalake_id, synonym, synonym_key, "
        + "deleted_at) VALUES (#{id},#{metalakeId},#{synonym},#{key},0)";
  }
  /** Returns SQL for clearSynonyms. */
  public static String clearSynonyms() {
    return "DELETE FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at=0";
  }
  /** Returns SQL for delete. */
  public static String delete() {
    return "UPDATE glossary_term_meta SET deleted_at=#{deletedAt} WHERE term_id=#{id} AND "
        + "current_version=#{version} AND deleted_at=0";
  }
  /** Returns SQL for deleteSynonyms. */
  public static String deleteSynonyms() {
    return "UPDATE glossary_term_synonym SET deleted_at=#{deletedAt} WHERE term_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for dropMetalake. */
  public static String dropMetalake() {
    return "UPDATE glossary_term_meta SET deleted_at=#{deletedAt} WHERE metalake_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for dropMetalakeSynonyms. */
  public static String dropMetalakeSynonyms() {
    return "UPDATE glossary_term_synonym SET deleted_at=#{deletedAt} WHERE metalake_id=#{id} AND "
        + "deleted_at=0";
  }
  /** Returns SQL for expired. */
  public static String expired() {
    return "SELECT term_id FROM glossary_term_meta WHERE deleted_at>0 AND deleted_at<#{cutoff} "
        + "ORDER BY term_id LIMIT #{limit}";
  }
  /** Returns SQL for purgeSynonyms. */
  public static String purgeSynonyms() {
    return "DELETE FROM glossary_term_synonym WHERE term_id=#{id} AND deleted_at>0";
  }
  /** Returns SQL for purge. */
  public static String purge() {
    return "DELETE FROM glossary_term_meta WHERE term_id=#{id} AND deleted_at>0";
  }
  /** Returns all live synonyms of a metalake, grouped by term and normalized key. */
  public static String listSynonyms() {
    return "SELECT term_id AS termId, synonym FROM glossary_term_synonym WHERE metalake_id=#{metalakeId} AND deleted_at=0 ORDER BY term_id,synonym_key";
  }

  private static String lock(boolean write) {
    String database =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    // H2 has no shared row-lock clause; use its exclusive lock for the same snapshot guarantee.
    if (write || "h2".equalsIgnoreCase(database)) {
      return " FOR UPDATE";
    }
    return "mysql".equalsIgnoreCase(database) ? " LOCK IN SHARE MODE" : " FOR SHARE";
  }
}
