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

import static org.apache.gravitino.storage.relational.mapper.PolicyRoleRelMapper.POLICY_ROLE_RELATION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.PolicyRoleRelPO;
import org.apache.ibatis.annotations.Param;

/** Base SQL provider for policy-to-role references. */
public class PolicyRoleRelBaseSQLProvider {

  /** Returns SQL for listing the role references of one policy. */
  public String listByPolicyId(@Param("policyId") Long policyId) {
    return selectColumns()
        + joins()
        + " WHERE prr.policy_id = #{policyId}"
        + activePredicates()
        + " ORDER BY prr.role_name";
  }

  /** Returns SQL for listing the policies that reference one role name. */
  public String listByRoleName(
      @Param("metalakeName") String metalakeName, @Param("roleName") String roleName) {
    return selectColumns()
        + joins()
        + " WHERE pm.metalake_id IN (SELECT mm.metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
        + " AND prr.role_name = #{roleName}"
        + activePredicates()
        + " ORDER BY pm.policy_name";
  }

  /** Returns SQL for inserting one reference. */
  public String insert(@Param("relation") PolicyRoleRelPO relation) {
    return "INSERT INTO "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + " (policy_id, role_name, current_version, last_version, deleted_at)"
        + " VALUES (#{relation.policyId}, #{relation.roleName},"
        + " #{relation.currentVersion}, #{relation.lastVersion}, #{relation.deletedAt})";
  }

  /** Returns SQL for soft-deleting one reference with a version CAS. */
  public String softDeleteByIdAndVersion(@Param("relation") PolicyRoleRelPO relation) {
    return "UPDATE "
        + POLICY_ROLE_RELATION_TABLE_NAME
        // Assign last_version first so MySQL computes both columns from the version observed
        // before this update.
        + " SET last_version = current_version + 1,"
        + " current_version = current_version + 1,"
        + " deleted_at = "
        + deletedAtNowExpression()
        + " WHERE id = #{relation.id} AND current_version = #{relation.currentVersion}"
        + " AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting references when a metalake is deleted. */
  public String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + deletedAtNowExpression()
        + " WHERE EXISTS (SELECT * FROM "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm WHERE pm.metalake_id = #{metalakeId} AND pm.policy_id = "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + ".policy_id)"
        + " AND deleted_at = 0";
  }

  /** Returns SQL for soft-deleting references when a policy is deleted or its content changes. */
  public String softDeleteByPolicyId(@Param("policyId") Long policyId) {
    return "UPDATE "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = "
        + deletedAtNowExpression()
        + " WHERE policy_id = #{policyId} AND deleted_at = 0";
  }

  /** Returns SQL for physically deleting expired reference rows. */
  public String deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  /** Returns the database expression for the current epoch-millisecond timestamp. */
  protected String deletedAtNowExpression() {
    return DatabaseTimeSQL.MYSQL;
  }

  private String selectColumns() {
    return "SELECT prr.id AS id, prr.policy_id AS policyId, pm.policy_name AS policyName,"
        + " prr.role_name AS roleName,"
        + " prr.current_version AS currentVersion, prr.last_version AS lastVersion,"
        + " prr.deleted_at AS deletedAt";
  }

  private String joins() {
    return " FROM "
        + POLICY_ROLE_RELATION_TABLE_NAME
        + " prr JOIN "
        + PolicyMetaMapper.POLICY_META_TABLE_NAME
        + " pm ON prr.policy_id = pm.policy_id";
  }

  private String activePredicates() {
    return " AND prr.deleted_at = 0 AND pm.deleted_at = 0";
  }
}
