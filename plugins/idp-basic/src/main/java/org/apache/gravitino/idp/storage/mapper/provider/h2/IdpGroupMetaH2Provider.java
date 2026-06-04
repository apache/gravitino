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

import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpGroupMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** SQL provider for IdP group metadata statements on H2 backends. */
public class IdpGroupMetaH2Provider extends IdpGroupMetaBaseSQLProvider {

  @Override
  public String selectIdpGroupWithUsers(@Param("groupName") String groupName) {
    return "SELECT g.group_name as name,"
        + " '['"
        + " || COALESCE(GROUP_CONCAT('\"' || u.user_name || '\"'), '')"
        + " || ']' as usernames"
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

  @Override
  protected String currentTimeMillisExpression() {
    return "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())";
  }
}
