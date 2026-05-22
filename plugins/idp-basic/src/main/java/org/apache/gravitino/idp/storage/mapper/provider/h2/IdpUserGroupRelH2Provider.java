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

package org.apache.gravitino.idp.storage.mapper.provider.h2;

import java.util.List;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserGroupRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** SQL provider for IdP user-group relation statements on H2 backends. */
public class IdpUserGroupRelH2Provider extends IdpUserGroupRelBaseSQLProvider {

  @Override
  protected String currentTimeMillisExpression() {
    return "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())";
  }

  @Override
  public String softDeleteRelations(
      @Param("groupName") String groupName, @Param("usernames") List<String> usernames) {
    return "<script>"
        + "UPDATE "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r SET deleted_at = "
        + currentTimeMillisExpression()
        + " WHERE r.deleted_at = 0"
        + " AND r.group_id IN (SELECT g.group_id FROM "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g WHERE g.group_name = #{groupName} AND g.deleted_at = 0)"
        + " AND r.user_id IN (SELECT u.user_id FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u WHERE u.deleted_at = 0"
        + "<foreach collection='usernames' item='username'"
        + " open=' AND u.user_name IN (' separator=',' close=')'>"
        + "#{username}"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  @Override
  public String softDeleteRelationsByUsername(@Param("username") String username) {
    return "MERGE INTO "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r USING "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u ON r.user_id = u.user_id"
        + " AND u.user_name = #{username}"
        + " AND u.deleted_at = 0"
        + " AND r.deleted_at = 0"
        + " WHEN MATCHED THEN UPDATE SET r.deleted_at = "
        + currentTimeMillisExpression();
  }

  @Override
  public String softDeleteRelationsByGroupName(@Param("groupName") String groupName) {
    return "MERGE INTO "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r USING "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g ON r.group_id = g.group_id"
        + " AND g.group_name = #{groupName}"
        + " AND g.deleted_at = 0"
        + " AND r.deleted_at = 0"
        + " WHEN MATCHED THEN UPDATE SET r.deleted_at = "
        + currentTimeMillisExpression();
  }
}
