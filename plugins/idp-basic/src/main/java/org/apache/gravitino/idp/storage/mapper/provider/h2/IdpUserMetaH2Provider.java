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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

/** SQL provider for IdP user metadata statements on H2 backends. */
public class IdpUserMetaH2Provider extends IdpUserMetaBaseSQLProvider {

  @Override
  public String selectIdpUserWithGroups(@Param("username") String username) {
    return "SELECT u.user_name as name, u.password_hash as passwordHash,"
        + " '['"
        + " || COALESCE(GROUP_CONCAT('\"' || g.group_name || '\"'), '')"
        + " || ']' as groupNames"
        + " FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " u LEFT JOIN "
        + IdpUserGroupRelMapper.IDP_USER_GROUP_REL_TABLE_NAME
        + " r ON r.user_id = u.user_id AND r.deleted_at = 0"
        + " LEFT JOIN "
        + IdpGroupMetaMapper.IDP_GROUP_TABLE_NAME
        + " g ON g.group_id = r.group_id AND g.deleted_at = 0"
        + " WHERE u.user_name = #{username} AND u.deleted_at = 0"
        + " GROUP BY u.user_id, u.user_name, u.password_hash";
  }

  @Override
  protected String currentTimeMillisExpression() {
    return "DATEDIFF('MILLISECOND', TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP())";
  }
}
