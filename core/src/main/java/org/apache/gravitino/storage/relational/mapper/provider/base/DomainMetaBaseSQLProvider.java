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

import static org.apache.gravitino.storage.relational.mapper.DomainMetaMapper.DOMAIN_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.ibatis.annotations.Param;

/** The SQL of the {@code domain_meta} table shared by all backends. */
public class DomainMetaBaseSQLProvider {

  private static final String DOMAIN_COLUMNS =
      "dm.domain_id as domainId, dm.domain_name as domainName,"
          + " dm.metalake_id as metalakeId,"
          + " dm.domain_comment as comment,"
          + " dm.domain_status as status,"
          + " dm.backing_tag_id as backingTagId,"
          + " dm.backing_tag_value as backingTagValue,"
          + " dm.properties as properties,"
          + " dm.audit_info as auditInfo,"
          + " dm.current_version as currentVersion,"
          + " dm.last_version as lastVersion,"
          + " dm.deleted_at as deletedAt";

  // Reads take the backing tag name from the live tag, so a deleted tag reads as no backing tag.
  // Locking reads do not join: they must lock the domain row only, and PostgreSQL rejects FOR
  // UPDATE on the nullable side of an outer join.
  private static final String DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME =
      DOMAIN_COLUMNS + ", bt.tag_name as backingTagName";

  private static final String BACKING_TAG_JOIN =
      " LEFT JOIN "
          + TagMetaMapper.TAG_TABLE_NAME
          + " bt ON dm.backing_tag_id = bt.tag_id AND dm.metalake_id = bt.metalake_id"
          + " AND bt.deleted_at = 0";

  /**
   * Returns SQL that lists the active domains of a metalake.
   *
   * @param metalakeName The metalake name.
   * @return The SQL.
   */
  public String listDomainPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT "
        + DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON dm.metalake_id = mm.metalake_id"
        + BACKING_TAG_JOIN
        + " WHERE mm.metalake_name = #{metalakeName} AND dm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  /**
   * Returns SQL that selects an active domain by metalake name and domain name.
   *
   * @param metalakeName The metalake name.
   * @param domainName The domain name.
   * @return The SQL.
   */
  public String selectDomainMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("domainName") String domainName) {
    return "SELECT "
        + DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON dm.metalake_id = mm.metalake_id"
        + BACKING_TAG_JOIN
        + " WHERE mm.metalake_name = #{metalakeName} AND dm.domain_name = #{domainName}"
        + " AND dm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  /**
   * Returns SQL that selects an active domain by metalake ID and domain name.
   *
   * @param metalakeId The metalake ID.
   * @param domainName The domain name.
   * @return The SQL.
   */
  public String selectDomainMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName) {
    return "SELECT "
        + DOMAIN_COLUMNS
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm WHERE dm.metalake_id = #{metalakeId} AND dm.domain_name = #{domainName}"
        + " AND dm.deleted_at = 0";
  }

  /**
   * Returns SQL that selects and exclusively locks an active domain by its natural key.
   *
   * @param metalakeId The metalake ID.
   * @param domainName The domain name.
   * @return The SQL.
   */
  public String selectDomainMetaByMetalakeIdAndNameForUpdate(
      @Param("metalakeId") Long metalakeId, @Param("domainName") String domainName) {
    return selectDomainMetaByMetalakeIdAndName(metalakeId, domainName) + " FOR UPDATE";
  }

  /**
   * Returns SQL that selects an active domain by ID.
   *
   * @param domainId The domain ID.
   * @return The SQL.
   */
  public String selectDomainMetaById(@Param("domainId") Long domainId) {
    return "SELECT "
        + DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm"
        + BACKING_TAG_JOIN
        + " WHERE dm.domain_id = #{domainId} AND dm.deleted_at = 0";
  }

  /**
   * Returns SQL that selects and exclusively locks an active domain by ID.
   *
   * @param domainId The domain ID.
   * @return The SQL.
   */
  public String selectDomainMetaByIdForUpdate(@Param("domainId") Long domainId) {
    return selectLockableDomainMetaById(domainId) + " FOR UPDATE";
  }

  /**
   * Returns SQL that selects an active domain by ID and locks it for shared access.
   *
   * @param domainId The domain ID.
   * @return The SQL.
   */
  public String selectDomainMetaByIdForShare(@Param("domainId") Long domainId) {
    return selectLockableDomainMetaById(domainId) + " LOCK IN SHARE MODE";
  }

  /**
   * Returns SQL that lists the active domains with the given IDs.
   *
   * @param domainIds The domain IDs.
   * @return The SQL.
   */
  public String listDomainPOsByDomainIds(@Param("domainIds") List<Long> domainIds) {
    return "<script>"
        + "SELECT "
        + DOMAIN_COLUMNS
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm WHERE dm.deleted_at = 0"
        + " AND dm.domain_id IN ("
        + "<foreach collection='domainIds' item='domainId' separator=','>"
        + "#{domainId}"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  /**
   * Returns SQL that lists the active domains of a metalake with the given names.
   *
   * @param metalakeName The metalake name.
   * @param domainNames The domain names.
   * @return The SQL.
   */
  public String listDomainPOsByMetalakeAndDomainNames(
      @Param("metalakeName") String metalakeName, @Param("domainNames") List<String> domainNames) {
    return "<script>"
        + "SELECT "
        + DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON dm.metalake_id = mm.metalake_id"
        + BACKING_TAG_JOIN
        + " WHERE mm.metalake_name = #{metalakeName}"
        + " AND dm.domain_name IN ("
        + "<foreach collection='domainNames' item='domainName' separator=','>"
        + "#{domainName}"
        + "</foreach>"
        + ")"
        + " AND dm.deleted_at = 0 AND mm.deleted_at = 0"
        + "</script>";
  }

  /**
   * Returns SQL that lists the active sub-domains of a domain.
   *
   * @param parentId The ID of the parent domain.
   * @return The SQL.
   */
  public String listDomainPOsByParentId(@Param("parentId") Long parentId) {
    return "SELECT "
        + DOMAIN_COLUMNS_WITH_BACKING_TAG_NAME
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm JOIN "
        + DomainRelMapper.DOMAIN_REL_TABLE_NAME
        + " dr ON dm.domain_id = dr.child_id AND dr.deleted_at = 0"
        + BACKING_TAG_JOIN
        + " WHERE dr.parent_id = #{parentId} AND dm.deleted_at = 0";
  }

  /**
   * Returns SQL that inserts a domain.
   *
   * @param domainPO The domain to insert.
   * @return The SQL.
   */
  public String insertDomainMeta(@Param("domainMeta") DomainPO domainPO) {
    return "INSERT INTO "
        + DOMAIN_TABLE_NAME
        + " (domain_id, domain_name, metalake_id, domain_comment, domain_status, backing_tag_id,"
        + " backing_tag_value, properties, audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{domainMeta.domainId},"
        + " #{domainMeta.domainName},"
        + " #{domainMeta.metalakeId},"
        + " #{domainMeta.comment},"
        + " #{domainMeta.status},"
        + " #{domainMeta.backingTagId},"
        + " #{domainMeta.backingTagValue},"
        + " #{domainMeta.properties},"
        + " #{domainMeta.auditInfo},"
        + " #{domainMeta.currentVersion},"
        + " #{domainMeta.lastVersion},"
        + " #{domainMeta.deletedAt}"
        + " )";
  }

  /**
   * Returns SQL that updates an active domain when its version still matches the observed one.
   *
   * @param newDomainPO The new domain row.
   * @param oldDomainPO The domain row observed by the caller.
   * @return The SQL.
   */
  public String updateDomainMeta(
      @Param("newDomainMeta") DomainPO newDomainPO, @Param("oldDomainMeta") DomainPO oldDomainPO) {
    return "UPDATE "
        + DOMAIN_TABLE_NAME
        + " SET domain_name = #{newDomainMeta.domainName},"
        + " domain_comment = #{newDomainMeta.comment},"
        + " domain_status = #{newDomainMeta.status},"
        + " backing_tag_id = #{newDomainMeta.backingTagId},"
        + " backing_tag_value = #{newDomainMeta.backingTagValue},"
        + " properties = #{newDomainMeta.properties},"
        + " audit_info = #{newDomainMeta.auditInfo},"
        + " current_version = #{newDomainMeta.currentVersion},"
        + " last_version = #{newDomainMeta.lastVersion},"
        + " deleted_at = #{newDomainMeta.deletedAt}"
        + " WHERE domain_id = #{oldDomainMeta.domainId}"
        + " AND current_version = #{oldDomainMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  /**
   * Returns SQL that soft-deletes a domain using its ID and observed version.
   *
   * @param domainId The domain ID.
   * @param currentVersion The version observed by the caller.
   * @return The SQL.
   */
  public String softDeleteDomainMetaByIdAndVersion(
      @Param("domainId") Long domainId, @Param("currentVersion") Long currentVersion) {
    return "UPDATE "
        + DOMAIN_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE domain_id = #{domainId} AND current_version = #{currentVersion}"
        + " AND deleted_at = 0";
  }

  /**
   * Returns SQL that soft-deletes all active domains of a metalake.
   *
   * @param metalakeId The metalake ID.
   * @return The SQL.
   */
  public String softDeleteDomainMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + DOMAIN_TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  /**
   * Returns SQL that hard-deletes domains soft-deleted before the given time.
   *
   * @param legacyTimeline The time before which soft-deleted rows are removed.
   * @param limit The maximum number of rows to remove.
   * @return The SQL.
   */
  public String deleteDomainMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + DOMAIN_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /**
   * Returns SQL that selects an active domain by ID without joining the backing tag, so that a
   * locking clause appended to it locks the domain row only.
   *
   * @param domainId The domain ID.
   * @return The SQL.
   */
  protected String selectLockableDomainMetaById(Long domainId) {
    return "SELECT "
        + DOMAIN_COLUMNS
        + " FROM "
        + DOMAIN_TABLE_NAME
        + " dm WHERE dm.domain_id = #{domainId} AND dm.deleted_at = 0";
  }
}
