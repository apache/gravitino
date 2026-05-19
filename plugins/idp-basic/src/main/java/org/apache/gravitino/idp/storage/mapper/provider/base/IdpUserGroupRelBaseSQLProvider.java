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
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpUserGroupRelPO;
import org.apache.ibatis.annotations.Param;

public class IdpUserGroupRelBaseSQLProvider {

  public String selectGroupNamesByUsername(@Param("username") String username) {
    return "SELECT g.group_name"
        + " FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u JOIN "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r ON r.user_id = u.user_id AND r.deleted_at = 0"
        + " JOIN "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g ON g.group_id = r.group_id AND g.deleted_at = 0"
        + " WHERE u.user_name = #{username}"
        + " AND u.deleted_at = 0"
        + " ORDER BY g.group_name";
  }

  public String selectUsernamesByGroupName(@Param("groupName") String groupName) {
    return "SELECT u.user_name"
        + " FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g JOIN "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r ON r.group_id = g.group_id AND r.deleted_at = 0"
        + " JOIN "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u ON u.user_id = r.user_id AND u.deleted_at = 0"
        + " WHERE g.group_name = #{groupName}"
        + " AND g.deleted_at = 0"
        + " ORDER BY u.user_name";
  }

  public String batchInsertRelations(@Param("relations") List<IdpUserGroupRelPO> relations) {
    return "<script>"
        + "INSERT INTO "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " (id, group_id, user_id, current_version, last_version, deleted_at)"
        + " VALUES "
        + "<foreach item='item' collection='relations' separator=','>"
        + "(#{item.id}, #{item.groupId}, #{item.userId}, #{item.currentVersion},"
        + " #{item.lastVersion}, #{item.deletedAt})"
        + "</foreach>"
        + "</script>";
  }

  public String softDeleteRelations(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds) {
    return "<script>"
        + "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " SET deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE group_id = #{groupId} AND deleted_at = 0 "
        + "<foreach collection='userIds' item='userId'"
        + " open='AND user_id IN (' separator=',' close=')'>"
        + "#{userId}"
        + "</foreach>"
        + "</script>";
  }

  public String softDeleteRelationsByUsername(@Param("username") String username) {
    return "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r INNER JOIN "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u ON u.user_id = r.user_id AND u.deleted_at = 0"
        + " SET r.deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE u.user_name = #{username}"
        + " AND r.deleted_at = 0";
  }

  public String softDeleteRelationsByGroupName(@Param("groupName") String groupName) {
    return "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r INNER JOIN "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g ON g.group_id = r.group_id AND g.deleted_at = 0"
        + " SET r.deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE g.group_name = #{groupName}"
        + " AND r.deleted_at = 0";
  }

  public String deleteIdpUserGroupRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  protected String currentTimeMillisExpression() {
    return "(UNIX_TIMESTAMP() * 1000.0)";
  }
}
