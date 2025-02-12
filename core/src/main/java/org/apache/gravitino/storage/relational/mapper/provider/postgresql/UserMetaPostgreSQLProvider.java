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

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserMetaMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.UserMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.ibatis.annotations.Param;

public class UserMetaPostgreSQLProvider extends UserMetaBaseSQLProvider {
  @Override
  public String softDeleteUserMetaByUserId(Long userId) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteUserMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + USER_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertUserMetaOnDuplicateKeyUpdate(UserPO userPO) {
    return "INSERT INTO "
        + USER_TABLE_NAME
        + "(user_id, user_name,"
        + "metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{userMeta.userId},"
        + " #{userMeta.userName},"
        + " #{userMeta.metalakeId},"
        + " #{userMeta.auditInfo},"
        + " #{userMeta.currentVersion},"
        + " #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt}"
        + " )"
        + " ON CONFLICT(user_id) DO UPDATE SET"
        + " user_name = #{userMeta.userName},"
        + " metalake_id = #{userMeta.metalakeId},"
        + " audit_info = #{userMeta.auditInfo},"
        + " current_version = #{userMeta.currentVersion},"
        + " last_version = #{userMeta.lastVersion},"
        + " deleted_at = #{userMeta.deletedAt}";
  }

  @Override
  public String listExtendedUserPOsByMetalakeId(Long metalakeId) {
    return "SELECT ut.user_id as userId, ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion, ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt,"
        + " JSON_AGG(rot.role_name) as roleNames,"
        + " JSON_AGG(rot.role_id) as roleIds"
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

  @Override
  public String deleteUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + USER_TABLE_NAME
        + " WHERE user_id IN (SELECT user_id FROM "
        + USER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
