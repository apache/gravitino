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

import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelBaseSQLProvider {

  public String batchInsertUserRoleRel(
      @Param("userRoleRelList") List<UserRoleRelPO> userRoleRelList) {
    return "<script> INSERT INTO "
        + USER_ROLE_RELATION_TABLE_NAME
        + " (user_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='userRoleRels' item='item' separator=','>"
        + "(#{item.userId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String batchInsertUserRoleRelOnDuplicateKeyUpdate(
      @Param("userRoleRels") List<UserRoleRelPO> userRoleRelPOs) {
    return "<script>"
        + "INSERT INTO "
        + USER_ROLE_RELATION_TABLE_NAME
        + " (user_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach collection='userRoleRels' item='item' separator=','>"
        + "(#{item.userId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})"
        + "</foreach>"
        + " ON DUPLICATE KEY UPDATE"
        + " user_id = VALUES(user_id),"
        + " role_id = VALUES(role_id),"
        + " audit_info = VALUES(audit_info),"
        + " current_version = VALUES(current_version),"
        + " last_version = VALUES(last_version),"
        + " deleted_at = VALUES(deleted_at)"
        + "</script>";
  }

  public String softDeleteUserRoleRelByUserId(@Param("userId") Long userId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String softDeleteUserRoleRelByUserAndRoles(
      @Param("userId") Long userId, @Param("roleIds") List<Long> roleIds) {
    return "<script>"
        + "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE user_id = #{userId} AND role_id IN ("
        + "<foreach collection='roleIds' item='roleId' separator=','>"
        + "#{roleId}"
        + "</foreach>"
        + " )"
        + " AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteUserRoleRelByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE user_id IN (SELECT user_id FROM "
        + USER_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteUserRoleRelByRoleId(@Param("roleId") Long roleId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
