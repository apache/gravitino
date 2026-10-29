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

import static org.apache.gravitino.storage.relational.mapper.DomainRelMapper.DOMAIN_REL_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.ibatis.annotations.Param;

/** The SQL of the {@code domain_rel} table shared by all backends. */
public class DomainRelBaseSQLProvider {

  private static final String DOMAIN_REL_COLUMNS =
      "dr.metalake_id as metalakeId, dr.parent_id as parentId, dr.child_id as childId,"
          + " dr.audit_info as auditInfo,"
          + " dr.current_version as currentVersion,"
          + " dr.last_version as lastVersion,"
          + " dr.deleted_at as deletedAt";

  /**
   * Returns SQL that selects the active parent edge of a domain.
   *
   * @param childId The ID of the domain.
   * @return The SQL.
   */
  public String selectDomainRelByChildId(@Param("childId") Long childId) {
    return "SELECT "
        + DOMAIN_REL_COLUMNS
        + " FROM "
        + DOMAIN_REL_TABLE_NAME
        + " dr WHERE dr.child_id = #{childId} AND dr.deleted_at = 0";
  }

  /**
   * Returns SQL that lists the active parent edges of the given domains, with the name of each
   * parent.
   *
   * @param childIds The IDs of the domains.
   * @return The SQL.
   */
  public String listDomainRelsByChildIds(@Param("childIds") List<Long> childIds) {
    return "<script>"
        + "SELECT "
        + DOMAIN_REL_COLUMNS
        + ", pd.domain_name as parentName"
        + " FROM "
        + DOMAIN_REL_TABLE_NAME
        + " dr JOIN "
        + DomainMetaMapper.DOMAIN_TABLE_NAME
        + " pd ON dr.parent_id = pd.domain_id AND pd.deleted_at = 0"
        + " WHERE dr.deleted_at = 0"
        + " AND dr.child_id IN ("
        + "<foreach collection='childIds' item='childId' separator=','>"
        + "#{childId}"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  /**
   * Returns SQL that counts the active sub-domains of a domain.
   *
   * @param parentId The ID of the parent domain.
   * @return The SQL.
   */
  public String countDomainRelsByParentId(@Param("parentId") Long parentId) {
    return "SELECT COUNT(*) FROM "
        + DOMAIN_REL_TABLE_NAME
        + " WHERE parent_id = #{parentId} AND deleted_at = 0";
  }

  /**
   * Returns SQL that inserts a parent edge.
   *
   * @param domainRelPO The edge to insert.
   * @return The SQL.
   */
  public String insertDomainRel(@Param("domainRel") DomainRelPO domainRelPO) {
    return "INSERT INTO "
        + DOMAIN_REL_TABLE_NAME
        + " (metalake_id, parent_id, child_id, audit_info, current_version, last_version,"
        + " deleted_at)"
        + " VALUES ("
        + " #{domainRel.metalakeId},"
        + " #{domainRel.parentId},"
        + " #{domainRel.childId},"
        + " #{domainRel.auditInfo},"
        + " #{domainRel.currentVersion},"
        + " #{domainRel.lastVersion},"
        + " #{domainRel.deletedAt}"
        + " )";
  }

  /**
   * Returns SQL that updates an active parent edge when its version still matches the observed one.
   *
   * @param newDomainRelPO The new edge.
   * @param oldDomainRelPO The edge observed by the caller.
   * @return The SQL.
   */
  public String updateDomainRel(
      @Param("newDomainRel") DomainRelPO newDomainRelPO,
      @Param("oldDomainRel") DomainRelPO oldDomainRelPO) {
    return "UPDATE "
        + DOMAIN_REL_TABLE_NAME
        + " SET parent_id = #{newDomainRel.parentId},"
        + " audit_info = #{newDomainRel.auditInfo},"
        + " current_version = #{newDomainRel.currentVersion},"
        + " last_version = #{newDomainRel.lastVersion},"
        + " deleted_at = #{newDomainRel.deletedAt}"
        + " WHERE child_id = #{oldDomainRel.childId}"
        + " AND parent_id = #{oldDomainRel.parentId}"
        + " AND current_version = #{oldDomainRel.currentVersion}"
        + " AND deleted_at = 0";
  }

  /**
   * Returns SQL that soft-deletes the active parent edge of a domain.
   *
   * @param childId The ID of the domain.
   * @return The SQL.
   */
  public String softDeleteDomainRelByChildId(@Param("childId") Long childId) {
    return "UPDATE "
        + DOMAIN_REL_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE child_id = #{childId} AND deleted_at = 0";
  }

  /**
   * Returns SQL that soft-deletes all active parent edges of a metalake.
   *
   * @param metalakeId The metalake ID.
   * @return The SQL.
   */
  public String softDeleteDomainRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + DOMAIN_REL_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  /**
   * Returns SQL that hard-deletes parent edges soft-deleted before the given time.
   *
   * @param legacyTimeline The time before which soft-deleted rows are removed.
   * @param limit The maximum number of rows to remove.
   * @return The SQL.
   */
  public String deleteDomainRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + DOMAIN_REL_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
