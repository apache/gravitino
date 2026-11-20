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

import java.util.Collection;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermMetaMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermRelMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermRelPO;
import org.apache.ibatis.annotations.Param;

/** SQL for glossary edges and the per-metalake hierarchy mutex, including MySQL 5.7. */
public class GlossaryTermRelBaseSQLProvider {
  private static final String COLUMNS =
      "r.metalake_id AS metalakeId, r.from_term_id AS fromTermId, "
          + "r.to_term_id AS toTermId, r.rel_type AS relType, r.deleted_at AS deletedAt";

  /** Creates the mutex row without aborting a concurrent first-use transaction. */
  public String ensureLock(@Param("metalakeId") long metalakeId) {
    return "INSERT INTO "
        + GlossaryTermRelMapper.HIERARCHY_LOCK_TABLE_NAME
        + " (metalake_id) VALUES (#{metalakeId}) "
        + "ON DUPLICATE KEY UPDATE metalake_id=metalake_id";
  }

  /** Locks the mutex until the caller transaction ends. */
  public String lockHierarchy(@Param("metalakeId") long metalakeId) {
    return "SELECT metalake_id FROM "
        + GlossaryTermRelMapper.HIERARCHY_LOCK_TABLE_NAME
        + " WHERE metalake_id=#{metalakeId} FOR UPDATE";
  }

  /** Locks one live endpoint after the metalake fence and hierarchy mutex. */
  public String lockTerm(@Param("metalakeId") long metalakeId, @Param("termId") long termId) {
    return "SELECT term_id FROM "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " WHERE metalake_id=#{metalakeId} "
        + "AND term_id=#{termId} AND deleted_at=0 FOR UPDATE";
  }

  /** Inserts an already canonicalized edge. */
  public String insert(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type) {
    return "INSERT INTO "
        + GlossaryTermRelMapper.TABLE_NAME
        + " (metalake_id, from_term_id, to_term_id, rel_type, deleted_at) "
        + "VALUES (#{metalakeId}, #{fromId}, #{toId}, #{type}, 0)";
  }

  /** Tests whether a live edge already exists under the hierarchy mutex. */
  public String exists(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type) {
    return "SELECT COUNT(*) FROM "
        + GlossaryTermRelMapper.TABLE_NAME
        + " WHERE "
        + "metalake_id=#{metalakeId} AND from_term_id=#{fromId} AND to_term_id=#{toId} AND rel_type=#{type}"
        + " AND deleted_at=0";
  }

  /** Retires one live edge. The caller supplies a collision-free tombstone time. */
  public String remove(
      @Param("metalakeId") long metalakeId,
      @Param("fromId") long fromId,
      @Param("toId") long toId,
      @Param("type") String type,
      @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermRelMapper.TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE "
        + "metalake_id=#{metalakeId} AND from_term_id=#{fromId} AND to_term_id=#{toId} AND rel_type=#{type}"
        + " AND deleted_at=0";
  }

  /** Reads the latest tombstone time under the metalake's hierarchy mutex or deletion fence. */
  public String lastDeletedAt(@Param("metalakeId") long metalakeId) {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM "
        + GlossaryTermRelMapper.TABLE_NAME
        + " WHERE metalake_id=#{metalakeId}";
  }

  /** Retires every edge touching a term in the caller's deletion transaction. */
  public String removeTerm(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermRelMapper.TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} "
        + "AND (from_term_id=#{termId} OR to_term_id=#{termId}) AND deleted_at=0";
  }

  /** Retires all live edges of a metalake under its exclusive deletion fence. */
  public String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + GlossaryTermRelMapper.TABLE_NAME
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND deleted_at=0";
  }

  /** Removes the mutex of a deleted metalake under its exclusive deletion fence. */
  public String dropLock(@Param("metalakeId") long metalakeId) {
    return "DELETE FROM "
        + GlossaryTermRelMapper.HIERARCHY_LOCK_TABLE_NAME
        + " WHERE metalake_id=#{metalakeId}";
  }

  /** Reads one frontier batch, excluding deleted endpoints, without recursive SQL or locks. */
  public String listEdges(
      @Param("metalakeId") long metalakeId,
      @Param("ids") Collection<Long> ids,
      @Param("type") String type,
      @Param("reverse") boolean reverse) {
    return "<script>SELECT "
        + COLUMNS
        + " FROM "
        + GlossaryTermRelMapper.TABLE_NAME
        + " r JOIN "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " f ON f.term_id=r.from_term_id AND f.metalake_id=r.metalake_id AND f.deleted_at=0 "
        + "JOIN "
        + GlossaryTermMetaMapper.TABLE_NAME
        + " t ON t.term_id=r.to_term_id AND t.metalake_id=r.metalake_id AND t.deleted_at=0 "
        + "JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " m ON m.metalake_id=r.metalake_id AND m.deleted_at=0 "
        + "WHERE r.metalake_id=#{metalakeId} AND r.deleted_at=0 AND r.rel_type=#{type} AND ("
        + "<choose><when test='reverse'>r.to_term_id</when><otherwise>r.from_term_id</otherwise></choose> IN "
        + "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach>"
        + "<if test='type == &quot;RELATED&quot;'> OR r.to_term_id IN "
        + "<foreach collection='ids' item='id' open='(' separator=',' close=')'>#{id}</foreach></if>"
        + ") ORDER BY r.from_term_id, r.to_term_id</script>";
  }

  /** Selects a bounded batch of retired edges for garbage collection. */
  public String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return "SELECT "
        + COLUMNS
        + " FROM "
        + GlossaryTermRelMapper.TABLE_NAME
        + " r WHERE r.deleted_at>0 "
        + "AND r.deleted_at<#{cutoff} ORDER BY r.deleted_at,r.from_term_id,r.to_term_id,r.rel_type LIMIT #{limit}";
  }

  /** Deletes only the selected retired generation, never a newly added live edge. */
  public String purge(@Param("relation") GlossaryTermRelPO relation) {
    return "DELETE FROM "
        + GlossaryTermRelMapper.TABLE_NAME
        + " WHERE metalake_id=#{relation.metalakeId} AND from_term_id=#{relation.fromTermId} "
        + "AND to_term_id=#{relation.toTermId} AND rel_type=#{relation.relType} AND deleted_at=#{relation.deletedAt} AND deleted_at>0";
  }
}
