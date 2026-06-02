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

import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.ibatis.annotations.Param;

public class IdpGroupMetaBaseSQLProvider {

  public String selectIdpGroup(@Param("groupName") String groupName) {
    return "SELECT group_id as groupId, group_name as groupName,"
        + " current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " WHERE group_name = #{groupName} AND deleted_at = 0";
  }

  public String selectIdpGroupWithUsers(@Param("groupName") String groupName) {
    return "SELECT g.group_name as name,"
        + " COALESCE(JSON_ARRAYAGG(u.user_name), JSON_ARRAY()) as usernames"
        + " FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g LEFT JOIN "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r ON r.group_id = g.group_id AND r.deleted_at = 0"
        + " LEFT JOIN "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u ON u.user_id = r.user_id AND u.deleted_at = 0"
        + " WHERE g.group_name = #{groupName} AND g.deleted_at = 0"
        + " GROUP BY g.group_id, g.group_name";
  }

  public String insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO) {
    return "INSERT INTO "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " (group_id, group_name, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{groupMeta.groupId},"
        + " #{groupMeta.groupName},"
        + " #{groupMeta.currentVersion},"
        + " #{groupMeta.lastVersion},"
        + " #{groupMeta.deletedAt}"
        + " )";
  }

  public String softDeleteIdpGroup(@Param("groupName") String groupName) {
    return "UPDATE "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " SET deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE group_name = #{groupName} AND deleted_at = 0";
  }

  public String deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  protected String currentTimeMillisExpression() {
    return "(UNIX_TIMESTAMP() * 1000.0)";
  }
}
