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
package org.apache.gravitino.storage.relational.mapper.provider.h2;

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserMetaMapper.USER_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.UserRoleRelMapper.USER_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.UserMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class UserMetaH2Provider extends UserMetaBaseSQLProvider {
  @Override
  public String listExtendedUserPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT ut.user_id as userId, ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion, ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt,"
        + " '[' || GROUP_CONCAT('\"' || rot.role_name || '\"') || ']' as roleNames,"
        + " '[' || GROUP_CONCAT('\"' || rot.role_id || '\"') || ']' as roleIds"
        + " FROM "
        + USER_TABLE_NAME
        + " ut LEFT OUTER JOIN "
        + USER_ROLE_RELATION_TABLE_NAME
        + " rt ON rt.user_id = ut.user_id"
        + " LEFT OUTER JOIN "
        + ROLE_TABLE_NAME
        + " rot ON rot.role_id = rt.role_id"
        + " WHERE "
        + " ut.deleted_at = 0 AND "
        + "(rot.deleted_at = 0 OR rot.deleted_at is NULL) AND "
        + "(rt.deleted_at = 0 OR rt.deleted_at is NULL) AND ut.metalake_id = #{metalakeId}"
        + " GROUP BY ut.user_id";
  }
}
