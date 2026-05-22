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

package org.apache.gravitino.idp.storage.mapper.provider.postgresql;

import java.util.List;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserGroupRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class IdpUserGroupRelPostgreSQLProvider extends IdpUserGroupRelBaseSQLProvider {

  @Override
  protected String currentTimeMillisExpression() {
    return "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";
  }

  @Override
  public String softDeleteRelations(
      @Param("groupName") String groupName, @Param("usernames") List<String> usernames) {
    return "<script>"
        + "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r SET deleted_at = "
        + currentTimeMillisExpression()
        + " FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g, "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u WHERE r.group_id = g.group_id"
        + " AND r.user_id = u.user_id"
        + " AND g.group_name = #{groupName}"
        + " AND g.deleted_at = 0"
        + " AND u.deleted_at = 0"
        + " AND r.deleted_at = 0"
        + "<foreach collection='usernames' item='username'"
        + " open=' AND u.user_name IN (' separator=',' close=')'>"
        + "#{username}"
        + "</foreach>"
        + "</script>";
  }

  @Override
  public String softDeleteRelationsByUsername(@Param("username") String username) {
    return "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r SET deleted_at = "
        + currentTimeMillisExpression()
        + " FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u WHERE r.user_id = u.user_id"
        + " AND u.user_name = #{username}"
        + " AND u.deleted_at = 0"
        + " AND r.deleted_at = 0";
  }

  @Override
  public String softDeleteRelationsByGroupName(@Param("groupName") String groupName) {
    return "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r SET deleted_at = "
        + currentTimeMillisExpression()
        + " FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g WHERE r.group_id = g.group_id"
        + " AND g.group_name = #{groupName}"
        + " AND g.deleted_at = 0"
        + " AND r.deleted_at = 0";
  }

  @Override
  public String deleteIdpUserGroupRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
