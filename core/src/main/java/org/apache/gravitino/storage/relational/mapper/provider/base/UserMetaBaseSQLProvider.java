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

import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserMetaMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.ibatis.annotations.Param;

public class UserMetaBaseSQLProvider {

  public String selectUserIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name) {
    return "SELECT user_id as userId FROM "
        + USER_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND user_name = #{userName}"
        + " AND deleted_at = 0";
  }

  public String selectUserMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name) {
    return "SELECT user_id as userId, user_name as userName,"
        + " metalake_id as metalakeId,"
        + " audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + USER_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND user_name = #{userName}"
        + " AND deleted_at = 0";
  }

  public String insertUserMeta(@Param("userMeta") UserPO userPO) {
    return "INSERT INTO "
        + USER_TABLE_NAME
        + " (user_id, user_name,"
        + " metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{userMeta.userId},"
        + " #{userMeta.userName},"
        + " #{userMeta.metalakeId},"
        + " #{userMeta.auditInfo},"
        + " #{userMeta.currentVersion},"
        + " #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt}"
        + " )";
  }

  public String insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO) {
    return "INSERT INTO "
        + USER_TABLE_NAME
        + " (user_id, user_name,"
        + " metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{userMeta.userId},"
        + " #{userMeta.userName},"
        + " #{userMeta.metalakeId},"
        + " #{userMeta.auditInfo},"
        + " #{userMeta.currentVersion},"
        + " #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " user_name = #{userMeta.userName},"
        + " metalake_id = #{userMeta.metalakeId},"
        + " audit_info = #{userMeta.auditInfo},"
        + " current_version = #{userMeta.currentVersion},"
        + " last_version = #{userMeta.lastVersion},"
        + " deleted_at = #{userMeta.deletedAt}";
  }

  public String softDeleteUserMetaByUserId(@Param("userId") Long userId) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET user_name = #{newUserMeta.userName},"
        + " metalake_id = #{newUserMeta.metalakeId},"
        + " audit_info = #{newUserMeta.auditInfo},"
        + " current_version = #{newUserMeta.currentVersion},"
        + " last_version = #{newUserMeta.lastVersion},"
        + " deleted_at = #{newUserMeta.deletedAt}"
        + " WHERE user_id = #{oldUserMeta.userId}"
        + " AND user_name = #{oldUserMeta.userName}"
        + " AND metalake_id = #{oldUserMeta.metalakeId}"
        + " AND audit_info = #{oldUserMeta.auditInfo}"
        + " AND current_version = #{oldUserMeta.currentVersion}"
        + " AND last_version = #{oldUserMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String listUsersByRoleId(@Param("roleId") Long roleId) {
    return "SELECT us.user_id as userId, us.user_name as userName,"
        + " us.metalake_id as metalakeId,"
        + " us.audit_info as auditInfo, us.current_version as currentVersion,"
        + " us.last_version as lastVersion, us.deleted_at as deletedAt"
        + " FROM "
        + USER_TABLE_NAME
        + " us JOIN "
        + USER_ROLE_RELATION_TABLE_NAME
        + " re ON us.user_id = re.user_id"
        + " WHERE re.role_id = #{roleId}"
        + " AND us.deleted_at = 0 AND re.deleted_at = 0";
  }

  public String listUserPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT ut.user_id as userId, ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion, ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt"
        + " FROM "
        + USER_TABLE_NAME
        + " ut JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mt ON ut.metalake_id = mt.metalake_id"
        + " WHERE mt.metalake_name = #{metalakeName}"
        + " AND ut.deleted_at = 0 AND mt.deleted_at = 0";
  }

  public String listExtendedUserPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT ut.user_id as userId, ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion, ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt,"
        + " JSON_ARRAYAGG(rot.role_name) as roleNames,"
        + " JSON_ARRAYAGG(rot.role_id) as roleIds"
        + " FROM "
        + USER_TABLE_NAME
        + " ut LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rt ON rt.user_id = ut.user_id"
        + " LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rot ON rot.role_id = rt.role_id"
        + " WHERE "
        + " ut.deleted_at = 0 AND"
        + " ut.metalake_id = #{metalakeId}"
        + " GROUP BY ut.user_id";
  }

  public String deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + USER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String touchUserUpdatedAt(@Param("userId") long userId) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET updated_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String getUserUpdatedAt(
      @Param("metalakeName") String metalakeName, @Param("userName") String userName) {
    return "SELECT um.user_id as userId, um.updated_at as updatedAt"
        + " FROM "
        + USER_TABLE_NAME
        + " um"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0"
        + " WHERE mm.metalake_name = #{metalakeName} AND um.user_name = #{userName}"
        + " AND um.deleted_at = 0";
  }

  /**
   * Builds the single-round-trip auth prefetch query for {@code (metalake, userName, groupNames)}.
   *
   * <p>The SQL is up to four {@code UNION ALL} branches that map 1:1 to the four {@code
   * org.apache.gravitino.storage.relational.po.auth.AuthPrefetchRow.Kind} values:
   *
   * <ul>
   *   <li>{@code 'USER'} branch — the request user's {@code user_meta} row.
   *   <li>{@code 'USER_ROLE'} branch — roles bound directly to the request user via {@code
   *       user_role_rel}; {@code parentId} carries the owning {@code user_id}.
   *   <li>{@code 'GROUP'} branch — the request user's groups (one row per name in {@code
   *       groupNames}).
   *   <li>{@code 'GROUP_ROLE'} branch — roles inherited via group membership through {@code
   *       group_role_rel}; {@code parentId} carries the owning {@code group_id}.
   * </ul>
   *
   * <p>The {@code GROUP} / {@code GROUP_ROLE} branches are appended only when {@code groupNames} is
   * non-empty, leaving a 2-branch query in the no-group case.
   *
   * <p>All branches must SELECT the same column list and aliases ({@code subjectType}, {@code
   * entityId}, {@code entityName}, {@code updatedAt}, {@code bindingOwnerId}) because {@code UNION
   * ALL} requires a uniform row shape. The aliases match {@code AuthPrefetchRow}'s field names so
   * MyBatis can reflectively map each row.
   *
   * @param metalakeName the metalake the user belongs to
   * @param userName the request user's name
   * @param groupNames the user's group memberships; may be empty
   * @return the SQL string
   */
  public String batchGetAuthSubjectsForUser(
      @Param("metalakeName") String metalakeName,
      @Param("userName") String userName,
      @Param("groupNames") List<String> groupNames) {
    // 'USER' branch → AuthPrefetchRow.Kind.USER.
    String userBranch =
        "SELECT 'USER' AS subjectType, um.user_id AS entityId, um.user_name AS entityName,"
            + " um.updated_at AS updatedAt, NULL AS bindingOwnerId"
            + " FROM "
            + USER_TABLE_NAME
            + " um"
            + " JOIN "
            + MetalakeMetaMapper.TABLE_NAME
            + " mm ON um.metalake_id = mm.metalake_id AND mm.deleted_at = 0"
            + " WHERE mm.metalake_name = #{metalakeName} AND um.user_name = #{userName}"
            + " AND um.deleted_at = 0";

    // 'USER_ROLE' branch → AuthPrefetchRow.Kind.USER_ROLE.
    // bindingOwnerId carries the owning user_id so consumers can bucket roles back to that user.
    String userRoleBranch =
        " UNION ALL "
            + "SELECT 'USER_ROLE' AS subjectType, rm.role_id AS entityId, rm.role_name AS entityName,"
            + " rm.updated_at AS updatedAt, ur.user_id AS bindingOwnerId"
            + " FROM "
            + USER_TABLE_NAME
            + " um"
            + " JOIN "
            + MetalakeMetaMapper.TABLE_NAME
            + " mm3 ON um.metalake_id = mm3.metalake_id AND mm3.deleted_at = 0"
            + " JOIN "
            + USER_ROLE_RELATION_TABLE_NAME
            + " ur ON ur.user_id = um.user_id AND ur.deleted_at = 0"
            + " JOIN "
            + ROLE_TABLE_NAME
            + " rm ON rm.role_id = ur.role_id AND rm.deleted_at = 0"
            + " WHERE mm3.metalake_name = #{metalakeName} AND um.user_name = #{userName}"
            + " AND um.deleted_at = 0";

    if (groupNames == null || groupNames.isEmpty()) {
      // No group memberships → 2-branch query (USER + USER_ROLE only).
      return "<script>" + userBranch + userRoleBranch + "</script>";
    }

    // 'GROUP' branch → AuthPrefetchRow.Kind.GROUP.
    String groupBranch =
        " UNION ALL "
            + "SELECT 'GROUP' AS subjectType, gm.group_id AS entityId, gm.group_name AS entityName,"
            + " gm.updated_at AS updatedAt, NULL AS bindingOwnerId"
            + " FROM "
            + GROUP_TABLE_NAME
            + " gm"
            + " JOIN "
            + MetalakeMetaMapper.TABLE_NAME
            + " mm2 ON gm.metalake_id = mm2.metalake_id AND mm2.deleted_at = 0"
            + " WHERE mm2.metalake_name = #{metalakeName}"
            + " AND gm.group_name IN"
            + " <foreach item='g' collection='groupNames' open='(' separator=',' close=')'>#{g}</foreach>"
            + " AND gm.deleted_at = 0";

    // 'GROUP_ROLE' branch → AuthPrefetchRow.Kind.GROUP_ROLE.
    // bindingOwnerId carries the owning group_id so consumers can bucket roles by group.
    String groupRoleBranch =
        " UNION ALL "
            + "SELECT 'GROUP_ROLE' AS subjectType, rm.role_id AS entityId, rm.role_name AS entityName,"
            + " rm.updated_at AS updatedAt, gr.group_id AS bindingOwnerId"
            + " FROM "
            + GROUP_TABLE_NAME
            + " gm"
            + " JOIN "
            + MetalakeMetaMapper.TABLE_NAME
            + " mm4 ON gm.metalake_id = mm4.metalake_id AND mm4.deleted_at = 0"
            + " JOIN "
            + GROUP_ROLE_RELATION_TABLE_NAME
            + " gr ON gr.group_id = gm.group_id AND gr.deleted_at = 0"
            + " JOIN "
            + ROLE_TABLE_NAME
            + " rm ON rm.role_id = gr.role_id AND rm.deleted_at = 0"
            + " WHERE mm4.metalake_name = #{metalakeName}"
            + " AND gm.group_name IN"
            + " <foreach item='g2' collection='groupNames' open='(' separator=',' close=')'>#{g2}</foreach>"
            + " AND gm.deleted_at = 0";

    return "<script>" + userBranch + userRoleBranch + groupBranch + groupRoleBranch + "</script>";
  }
}
