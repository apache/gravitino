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

import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.GroupRoleRelMapper.GROUP_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.ibatis.annotations.Param;

public class GroupRoleRelBaseSQLProvider {
  public String batchInsertGroupRoleRel(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return "<script>"
        + "INSERT INTO "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " (group_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='groupRoleRels' item='item' separator=','>"
        + "(#{item.groupId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String batchInsertGroupRoleRelOnDuplicateKeyUpdate(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS) {
    return "<script>"
        + "INSERT INTO "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " (group_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='groupRoleRels' item='item' separator=','>"
        + "(#{item.groupId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + " ON DUPLICATE KEY UPDATE"
        + " group_id = VALUES(group_id),"
        + " role_id = VALUES(role_id),"
        + " audit_info = VALUES(audit_info),"
        + " current_version = VALUES(current_version),"
        + " last_version = VALUES(last_version),"
        + " deleted_at = VALUES(deleted_at)"
        + "</script>";
  }

  public String softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  public String softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds) {
    return "<script>"
        + "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE group_id = #{groupId} AND role_id in ("
        + "<foreach collection='roleIds' item='roleId' separator=','>"
        + "#{roleId}"
        + "</foreach>"
        + ") "
        + "AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteGroupRoleRelByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE group_id IN (SELECT group_id FROM "
        + GROUP_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteGroupRoleRelByRoleId(Long roleId) {
    return "UPDATE "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String deleteGroupRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
