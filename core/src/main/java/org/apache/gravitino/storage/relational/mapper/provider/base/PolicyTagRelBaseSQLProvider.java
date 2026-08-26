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

import static org.apache.gravitino.storage.relational.mapper.PolicyTagRelMapper.POLICY_TAG_RELATION_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.ibatis.annotations.Param;

/** Base SQL provider for policy-to-tag relations. */
public class PolicyTagRelBaseSQLProvider {

  /** Returns SQL for listing relations anchored by tag names. */
  public String listByTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames) {
    return listRelations("tm.metalake_id", "tm.tag_name", "tagNames", "tagName");
  }

  /** Returns SQL for listing relations anchored by policy names. */
  public String listByPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames) {
    return listRelations("pm.metalake_id", "pm.policy_name", "policyNames", "policyName");
  }

  /** Returns SQL for getting one active relation. */
  public String getByPolicyIdAndTagId(
      @Param("policyId") Long policyId, @Param("tagId") Long tagId) {
    return selectColumns()
        + joins()
        + " WHERE ptr.policy_id = #{policyId} AND ptr.tag_id = #{tagId}"
        + activePredicates();
  }

  /** Returns SQL for inserting one relation. */
  public String insert(@Param("relation") PolicyTagRelPO relation) {
    return "INSERT INTO "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " (policy_id, tag_id, selector, audit_info, current_version, last_version, deleted_at)"
        + " VALUES (#{relation.policyId}, #{relation.tagId}, #{relation.selector},"
        + " #{relation.auditInfo}, #{relation.currentVersion}, #{relation.lastVersion},"
        + " #{relation.deletedAt})";
  }

  /** Returns SQL for soft-deleting one relation. */
  public String softDeleteByPair(@Param("relation") PolicyTagRelPO relation) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + deletedAtNowExpression()
        + " WHERE policy_id = #{relation.policyId} AND tag_id = #{relation.tagId}"
        + " AND current_version = #{relation.currentVersion} AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting relations by tag ID. */
  public String softDeleteByTagId(@Param("tagId") Long tagId) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + deletedAtNowExpression()
        + " WHERE tag_id = #{tagId} AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting relations when a metalake is deleted. */
  public String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + deletedAtNowExpression()
        + " WHERE EXISTS (SELECT * FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id = #{metalakeId} AND pm.policy_id = "
        + POLICY_TAG_RELATION_TABLE_NAME
        + ".policy_id)"
        + " AND deleted_at = 0";
  }

  /** Returns SQL for physically deleting expired relation rows. */
  public String deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns the database expression for the current epoch-millisecond timestamp. */
  protected String deletedAtNowExpression() {
    return "(UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000";
  }

  private String listRelations(
      String metalakeIdColumn, String nameColumn, String collection, String item) {
    return "<script>"
        + selectColumns()
        + joins()
        + " WHERE "
        + metalakeIdColumn
        + " IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND "
        + nameColumn
        + " IN <foreach item='"
        + item
        + "' collection='"
        + collection
        + "' open='(' separator=',' close=')'>#{"
        + item
        + "}</foreach>"
        + activePredicates()
        + " ORDER BY tm.tag_name, pm.policy_name"
        + "</script>";
  }

  private String selectColumns() {
    return "SELECT ptr.policy_id AS policyId, pm.policy_name AS policyName,"
        + " ptr.tag_id AS tagId, tm.tag_name AS tagName, ptr.selector,"
        + " ptr.audit_info AS auditInfo, ptr.current_version AS currentVersion,"
        + " ptr.last_version AS lastVersion, ptr.deleted_at AS deletedAt";
  }

  private String joins() {
    return " FROM "
        + POLICY_TAG_RELATION_TABLE_NAME
        + " ptr JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm ON ptr.policy_id = pm.policy_id JOIN "
        + TagMetaMapper.TAG_TABLE_NAME
        + " tm ON ptr.tag_id = tm.tag_id";
  }

  private String activePredicates() {
    return " AND ptr.deleted_at = 0 AND pm.deleted_at = 0 AND tm.deleted_at = 0";
  }
}
