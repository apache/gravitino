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

import static org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper.IDP_GROUP_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper.IDP_USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.ibatis.annotations.Param;

public class IdpGroupUserRelBaseSQLProvider {

  public String selectGroupNamesByUserId(@Param("userId") Long userId) {
    return "SELECT g.group_name"
        + " FROM "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " r JOIN "
        + IDP_GROUP_TABLE_NAME
        + " g ON g.group_id = r.group_id"
        + " WHERE r.user_id = #{userId}"
        + " AND r.deleted_at = 0"
        + " AND g.deleted_at = 0"
        + " ORDER BY g.group_name";
  }

  public String selectUserNamesByGroupId(@Param("groupId") Long groupId) {
    return "SELECT u.user_name"
        + " FROM "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " r JOIN "
        + IDP_USER_TABLE_NAME
        + " u ON u.user_id = r.user_id"
        + " WHERE r.group_id = #{groupId}"
        + " AND r.deleted_at = 0"
        + " AND u.deleted_at = 0"
        + " ORDER BY u.user_name";
  }

  public String selectRelatedUserIds(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return "<script>"
        + "SELECT user_id"
        + " FROM "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " WHERE group_id = #{groupId} "
        + "<choose>"
        + "<when test='userIds != null and userIds.size() > 0'>"
        + "AND user_id IN ("
        + "<foreach collection='userIds' item='userId' separator=','>"
        + "#{userId}"
        + "</foreach>"
        + ") "
        + "</when>"
        + "<otherwise>"
        + "AND 1 = 0 "
        + "</otherwise>"
        + "</choose>"
        + "AND deleted_at = 0"
        + "</script>";
  }

  public String batchInsertLocalGroupUsers(@Param("relations") List<IdpGroupUserRelPO> relations) {
    return "<script>"
        + "INSERT INTO "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " (id, group_id, user_id, audit_info, current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach item='item' collection='relations' separator=','>"
        + "(#{item.id}, #{item.groupId}, #{item.userId}, #{item.auditInfo}, "
        + "#{item.currentVersion}, #{item.lastVersion}, #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String softDeleteLocalGroupUsers(
      @Param("groupId") Long groupId,
      @Param("userIds") List<Long> userIds,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return "<script>"
        + "UPDATE "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = #{deletedAt},"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE group_id = #{groupId} "
        + "<choose>"
        + "<when test='userIds != null and userIds.size() > 0'>"
        + "AND user_id IN ("
        + "<foreach collection='userIds' item='userId' separator=','>"
        + "#{userId}"
        + "</foreach>"
        + ") "
        + "</when>"
        + "<otherwise>"
        + "AND 1 = 0 "
        + "</otherwise>"
        + "</choose>"
        + "AND deleted_at = 0"
        + "</script>";
  }

  public String softDeleteGroupUsersByUserId(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return "UPDATE "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = #{deletedAt},"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String softDeleteGroupUsersByGroupId(
      @Param("groupId") Long groupId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return "UPDATE "
        + IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = #{deletedAt},"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  public String truncateLocalGroupUserRel() {
    return "DELETE FROM " + IDP_GROUP_USER_REL_TABLE_NAME;
  }
}
