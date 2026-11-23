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
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermDomainRelMapper;
import org.apache.gravitino.storage.relational.po.GlossaryTermDomainRelPO;
import org.apache.ibatis.annotations.Param;

/** Portable SQL for glossary term domain membership on H2, MySQL and PostgreSQL. */
public class GlossaryTermDomainRelBaseSQLProvider {
  private static final String TABLE = GlossaryTermDomainRelMapper.TABLE_NAME;

  /** Inserts a live membership row. */
  public String insert(
      @Param("metalakeId") long metalakeId,
      @Param("termId") long termId,
      @Param("domainId") long domainId) {
    return "INSERT INTO "
        + TABLE
        + " (metalake_id, term_id, domain_id, deleted_at)"
        + " VALUES (#{metalakeId}, #{termId}, #{domainId}, 0)";
  }

  /** Retires one live membership row. */
  public String remove(
      @Param("termId") long termId,
      @Param("domainId") long domainId,
      @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt}"
        + " WHERE term_id=#{termId} AND domain_id=#{domainId} AND deleted_at=0";
  }

  /** Retires every live membership row of a term. */
  public String removeTerm(@Param("termId") long termId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt} WHERE term_id=#{termId} AND deleted_at=0";
  }

  /** Retires every live membership row of a metalake. */
  public String dropMetalake(
      @Param("metalakeId") long metalakeId, @Param("deletedAt") long deletedAt) {
    return "UPDATE "
        + TABLE
        + " SET deleted_at=#{deletedAt} WHERE metalake_id=#{metalakeId} AND deleted_at=0";
  }

  /** Reads the latest tombstone time of a term. */
  public String lastDeletedAt(@Param("termId") long termId) {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM " + TABLE + " WHERE term_id=#{termId}";
  }

  /** Reads the latest tombstone time in a metalake. */
  public String lastDeletedAtInMetalake(@Param("metalakeId") long metalakeId) {
    return "SELECT COALESCE(MAX(deleted_at), 0) FROM " + TABLE + " WHERE metalake_id=#{metalakeId}";
  }

  /** Lists live memberships of a batch of terms, joined to live domains, in a stable order. */
  public String listDomainNames(
      @Param("metalakeId") long metalakeId, @Param("termIds") Collection<Long> termIds) {
    return "<script>SELECT r.term_id AS termId, r.domain_id AS domainId,"
        + " d.domain_name AS domainName FROM "
        + TABLE
        + " r JOIN "
        + DomainMetaMapper.DOMAIN_TABLE_NAME
        + " d ON d.domain_id=r.domain_id AND d.metalake_id=r.metalake_id AND d.deleted_at=0"
        + " WHERE r.metalake_id=#{metalakeId} AND r.deleted_at=0 AND r.term_id IN "
        + "<foreach collection='termIds' item='id' open='(' separator=',' close=')'>#{id}</foreach>"
        + " ORDER BY r.term_id, d.domain_name</script>";
  }

  /** Lists the live domain IDs of a term. */
  public String listDomainIds(@Param("termId") long termId) {
    return "SELECT domain_id FROM " + TABLE + " WHERE term_id=#{termId} AND deleted_at=0";
  }

  /** Lists the live member terms of a set of domains. */
  public String listTermIds(
      @Param("metalakeId") long metalakeId, @Param("domainIds") Collection<Long> domainIds) {
    return "<script>SELECT DISTINCT term_id FROM "
        + TABLE
        + " WHERE metalake_id=#{metalakeId} AND deleted_at=0 AND domain_id IN "
        + "<foreach collection='domainIds' item='id' open='(' separator=',' close=')'>#{id}"
        + "</foreach></script>";
  }

  /** Counts the live member terms of a domain. */
  public String countTerms(@Param("domainId") long domainId) {
    return "SELECT COUNT(*) FROM " + TABLE + " WHERE domain_id=#{domainId} AND deleted_at=0";
  }

  /** Lists the live direct sub-domain IDs of a domain. */
  public String listSubdomainIds(
      @Param("metalakeId") long metalakeId, @Param("domainId") long domainId) {
    return "SELECT r.child_id FROM "
        + DomainRelMapper.DOMAIN_REL_TABLE_NAME
        + " r JOIN "
        + DomainMetaMapper.DOMAIN_TABLE_NAME
        + " d ON d.domain_id=r.child_id AND d.deleted_at=0"
        + " WHERE r.metalake_id=#{metalakeId} AND r.parent_id=#{domainId} AND r.deleted_at=0"
        + " ORDER BY d.domain_name";
  }

  /** Selects a bounded batch of retired rows for garbage collection. */
  public String expired(@Param("cutoff") long cutoff, @Param("limit") int limit) {
    return "SELECT metalake_id AS metalakeId, term_id AS termId, domain_id AS domainId,"
        + " deleted_at AS deletedAt FROM "
        + TABLE
        + " WHERE deleted_at>0 AND deleted_at<#{cutoff}"
        + " ORDER BY deleted_at, term_id, domain_id LIMIT #{limit}";
  }

  /** Deletes only the selected retired generation. */
  public String purge(@Param("row") GlossaryTermDomainRelPO row) {
    return "DELETE FROM "
        + TABLE
        + " WHERE term_id=#{row.termId} AND domain_id=#{row.domainId}"
        + " AND deleted_at=#{row.deletedAt} AND deleted_at>0";
  }
}
