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

import static org.apache.gravitino.storage.relational.mapper.IdpUserMetaMapper.IDP_USER_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaBaseSQLProvider {

  public String selectIdpUser(@Param("userName") String userName) {
    return "SELECT user_id as userId, user_name as userName, password_hash as passwordHash,"
        + " audit_info as auditInfo, current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + IDP_USER_TABLE_NAME
        + " WHERE user_name = #{userName} AND deleted_at = 0";
  }

  public String selectIdpUsers(@Param("userNames") List<String> userNames) {
    return "<script>"
        + "SELECT user_id as userId, user_name as userName, password_hash as passwordHash,"
        + " audit_info as auditInfo, current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + IDP_USER_TABLE_NAME
        + " WHERE deleted_at = 0 AND user_name IN "
        + "<foreach item='item' collection='userNames' open='(' separator=',' close=')'>"
        + "#{item}"
        + "</foreach>"
        + "</script>";
  }

  public String insertIdpUser(@Param("userMeta") IdpUserPO userPO) {
    return "INSERT INTO "
        + IDP_USER_TABLE_NAME
        + " (user_id, user_name, password_hash,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{userMeta.userId},"
        + " #{userMeta.userName},"
        + " #{userMeta.passwordHash},"
        + " #{userMeta.auditInfo},"
        + " #{userMeta.currentVersion},"
        + " #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt}"
        + " )";
  }

  public String updateIdpUserPassword(
      @Param("userId") Long userId,
      @Param("passwordHash") String passwordHash,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") Long currentVersion,
      @Param("newCurrentVersion") Long newCurrentVersion,
      @Param("newLastVersion") Long newLastVersion) {
    return "UPDATE "
        + IDP_USER_TABLE_NAME
        + " SET password_hash = #{passwordHash},"
        + " audit_info = #{auditInfo},"
        + " current_version = #{newCurrentVersion},"
        + " last_version = #{newLastVersion}"
        + " WHERE user_id = #{userId}"
        + " AND current_version = #{currentVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeleteIdpUser(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo) {
    return "UPDATE "
        + IDP_USER_TABLE_NAME
        + " SET deleted_at = #{deletedAt},"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IDP_USER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
