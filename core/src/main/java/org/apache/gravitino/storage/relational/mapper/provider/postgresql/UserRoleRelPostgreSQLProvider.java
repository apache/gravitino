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

package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.UserRoleRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.UserRoleRelPO;
import org.apache.ibatis.annotations.Param;

public class UserRoleRelPostgreSQLProvider extends UserRoleRelBaseSQLProvider {
  @Override
  public String softDeleteUserRoleRelByUserId(Long userId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteUserRoleRelByUserAndRoles(Long userId, List<Long> roleIds) {
    return "<script>"
        + "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE user_id = #{userId} AND role_id in ("
        + "<foreach collection='roleIds' item='roleId' separator=','>"
        + "#{roleId}"
        + "</foreach>"
        + ") "
        + "AND deleted_at = 0"
        + "</script>";
  }

  @Override
  public String softDeleteUserRoleRelByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE user_id IN (SELECT user_id FROM "
        + USER_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteUserRoleRelByRoleId(Long roleId) {
    return "UPDATE "
        + USER_ROLE_RELATION_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + "  timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  @Override
  public String batchInsertUserRoleRelOnDuplicateKeyUpdate(List<UserRoleRelPO> userRoleRelPOs) {
    return "<script>"
        + "INSERT INTO "
        + USER_ROLE_RELATION_TABLE_NAME
        + "(user_id, role_id,"
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
        + " ON CONFLICT (user_id, role_id, deleted_at) DO UPDATE SET"
        + " user_id = VALUES(user_id),"
        + " role_id = VALUES(role_id),"
        + " audit_info = VALUES(audit_info),"
        + " current_version = VALUES(current_version),"
        + " last_version = VALUES(last_version),"
        + " deleted_at = VALUES(deleted_at)"
        + "</script>";
  }

  @Override
  public String deleteUserRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
