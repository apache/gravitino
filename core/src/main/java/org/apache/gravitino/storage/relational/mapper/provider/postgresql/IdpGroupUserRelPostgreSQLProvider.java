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

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.IdpGroupUserRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class IdpGroupUserRelPostgreSQLProvider extends IdpGroupUserRelBaseSQLProvider {

  @Override
  public String softDeleteIdpGroupUsers(
      Long groupId, List<Long> userIds, Long deletedAt, String auditInfo) {
    return "<script>"
        + "UPDATE "
        + IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT),"
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

  @Override
  public String softDeleteGroupUsersByUserId(Long userId, Long deletedAt, String auditInfo) {
    return "UPDATE "
        + IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT),"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteGroupUsersByGroupId(Long groupId, Long deletedAt, String auditInfo) {
    return "UPDATE "
        + IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT),"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  @Override
  public String deleteIdpGroupUserRelMetasByLegacyTimeline(
      Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + IdpGroupUserRelMapper.IDP_GROUP_USER_REL_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
