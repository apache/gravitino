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

package org.apache.gravitino.idp.storage.mapper.provider.base;

import java.util.List;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaBaseSQLProvider {
  public String selectIdpUser(@Param("username") String username) {
    return "SELECT user_id as userId, user_name as username, password_hash as passwordHash,"
        + " current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " WHERE user_name = #{username} AND deleted_at = 0";
  }

  public String selectIdpUsers(@Param("usernames") List<String> usernames) {
    return "<script>"
        + "SELECT user_id as userId, user_name as username, password_hash as passwordHash,"
        + " current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " WHERE deleted_at = 0 "
        + "<foreach collection='usernames' item='username'"
        + " open='AND user_name IN (' separator=',' close=')'>"
        + "#{username}"
        + "</foreach>"
        + "</script>";
  }

  public String insertIdpUser(@Param("userMeta") IdpUserPO userPO) {
    return "INSERT INTO "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " (user_id, user_name, password_hash, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{userMeta.userId},"
        + " #{userMeta.username},"
        + " #{userMeta.passwordHash},"
        + " #{userMeta.currentVersion},"
        + " #{userMeta.lastVersion},"
        + " #{userMeta.deletedAt}"
        + " )";
  }

  public String updateIdpUserPassword(
      @Param("userId") Long userId, @Param("passwordHash") String passwordHash) {
    return "UPDATE "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " SET password_hash = #{passwordHash}"
        + " WHERE user_id = #{userId}"
        + " AND deleted_at = 0";
  }

  public String softDeleteIdpUser(@Param("userId") Long userId) {
    return "UPDATE "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " SET deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  public String deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  protected String currentTimeMillisExpression() {
    return "(UNIX_TIMESTAMP() * 1000.0)";
  }
}
