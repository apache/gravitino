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

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserMetaMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

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
}
