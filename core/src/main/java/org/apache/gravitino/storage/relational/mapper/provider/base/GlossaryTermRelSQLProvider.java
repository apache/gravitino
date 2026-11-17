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

import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.ibatis.builder.annotation.ProviderContext;

/** SQL for glossary edges and the per-metalake hierarchy mutex, including MySQL 5.7. */
public final class GlossaryTermRelSQLProvider {
  private static final String COLUMNS =
      "r.metalake_id AS metalakeId, r.from_term_id AS fromTermId, "
          + "r.to_term_id AS toTermId, r.rel_type AS relType, r.deleted_at AS deletedAt";
  private static final String KEY =
      "metalake_id=#{metalakeId} AND from_term_id=#{fromId} AND to_term_id=#{toId} "
          + "AND rel_type=#{type}";

  private GlossaryTermRelSQLProvider() {}

  /** Creates the mutex row without aborting a concurrent first-use transaction. */
  public static String ensureLock(ProviderContext context) {
    switch (JDBCBackendType.fromString(context.getDatabaseId())) {
      case MYSQL:
        return "INSERT INTO glossary_hierarchy_lock (metalake_id) VALUES (#{metalakeId}) "
            + "ON DUPLICATE KEY UPDATE metalake_id=VALUES(metalake_id)";
      case POSTGRESQL:
        return "INSERT INTO glossary_hierarchy_lock (metalake_id) VALUES (#{metalakeId}) "
            + "ON CONFLICT (metalake_id) DO NOTHING";
      case H2:
        return "MERGE INTO glossary_hierarchy_lock (metalake_id) KEY (metalake_id) "
            + "VALUES (#{metalakeId})";
      default:
        throw new UnsupportedOperationException(
            "Unsupported glossary database: " + context.getDatabaseId());
    }
  }

  /** Locks the mutex until the caller transaction ends. */
  public static String lockHierarchy() {
    return "SELECT metalake_id FROM glossary_hierarchy_lock WHERE metalake_id=#{metalakeId} FOR UPDATE";
  }

  /** Locks one live endpoint after the metalake fence and hierarchy mutex. */
  public static String lockTerm() {
    return "SELECT term_id FROM glossary_term_meta WHERE metalake_id=#{metalakeId} "
        + "AND term_id=#{termId} AND deleted_at=0 FOR UPDATE";
  }

  /** Inserts an already canonicalized edge. */
  public static String insert() {
    return "INSERT INTO glossary_term_rel (metalake_id, from_term_id, to_term_id, rel_type, deleted_at) "
        + "VALUES (#{metalakeId}, #{fromId}, #{toId}, #{type}, 0)";
  }

  /** Tests whether a live edge already exists under the hierarchy mutex. */
  public static String exists() {
    return "SELECT COUNT(*) FROM glossary_term_rel WHERE " + KEY + " AND deleted_at=0";
  }

  /** Retires one live edge. The caller supplies a collision-free tombstone time. */
  public static String remove() {
    return "UPDATE glossary_term_rel SET deleted_at=#{deletedAt} WHERE "
        + KEY
        + " AND deleted_at=0";
  }

  /** Reads the latest tombstone time under the metalake's hierarchy mutex or deletion fence. */
  public static String lastDeletedAt() {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM glossary_term_rel WHERE metalake_id=#{metalakeId}";
  }

  /** Retires every edge touching a term in the caller's deletion transaction. */
  public static String removeTerm() {
    return "UPDATE glossary_term_rel SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} "
        + "AND (from_term_id=#{termId} OR to_term_id=#{termId}) AND deleted_at=0";
  }

  /** Retires all live edges of a metalake under its exclusive deletion fence. */
  public static String dropMetalake() {
    return "UPDATE glossary_term_rel SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND deleted_at=0";
  }

  /** Removes the mutex of a deleted metalake under its exclusive deletion fence. */
  public static String dropLock() {
    return "DELETE FROM glossary_hierarchy_lock WHERE metalake_id=#{metalakeId}";
  }

  /** Reads one complete frontier, excluding deleted endpoints, without recursive SQL or locks. */
  public static String listEdges() {
    return "<script>SELECT "
        + COLUMNS
        + " FROM glossary_term_rel r "
        + "JOIN glossary_term_meta f ON f.term_id=r.from_term_id AND f.metalake_id=r.metalake_id AND f.deleted_at=0 "
        + "JOIN glossary_term_meta t ON t.term_id=r.to_term_id AND t.metalake_id=r.metalake_id AND t.deleted_at=0 "
        + "JOIN metalake_meta m ON m.metalake_id=r.metalake_id AND m.deleted_at=0 "
        + "WHERE r.metalake_id=#{metalakeId} AND r.deleted_at=0 AND r.rel_type=#{type} AND ("
        + "<choose><when test='reverse'>r.to_term_id</when><otherwise>r.from_term_id</otherwise></choose> IN "
        + "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>"
        + "<if test='type == &quot;RELATED&quot;'> OR r.to_term_id IN "
        + "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach></if>"
        + ") ORDER BY r.from_term_id, r.to_term_id</script>";
  }

  /** Selects a bounded batch of retired edges for garbage collection. */
  public static String expired() {
    return "SELECT "
        + COLUMNS
        + " FROM glossary_term_rel r WHERE r.deleted_at>0 "
        + "AND r.deleted_at<#{cutoff} ORDER BY r.deleted_at,r.from_term_id,r.to_term_id,r.rel_type LIMIT #{limit}";
  }

  /** Deletes only the selected retired generation, never a newly added live edge. */
  public static String purge() {
    return "DELETE FROM glossary_term_rel WHERE metalake_id=#{p.metalakeId} AND from_term_id=#{p.fromTermId} "
        + "AND to_term_id=#{p.toTermId} AND rel_type=#{p.relType} AND deleted_at=#{p.deletedAt} AND deleted_at>0";
  }
}
