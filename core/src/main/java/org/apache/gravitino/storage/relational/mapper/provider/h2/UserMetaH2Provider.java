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
        + " ut LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rt ON rt.user_id = ut.user_id"
        + " LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rot ON rot.role_id = rt.role_id"
        + " WHERE "
        + " ut.deleted_at = 0 AND"
        + " ut.metalake_id = #{metalakeId}"
        + " GROUP BY ut.user_id";
  }
<<<<<<< HEAD
=======

  @Override
  public String listExtendedUserPOsByMetalakeNamePaginated(
      @Param("metalakeName") String metalakeName,
      @Param("offset") int offset,
      @Param("limit") int limit) {
    return "SELECT ut.user_id as userId, ut.user_name as userName,"
        + " ut.metalake_id as metalakeId,"
        + " ut.audit_info as auditInfo,"
        + " ut.current_version as currentVersion, ut.last_version as lastVersion,"
        + " ut.deleted_at as deletedAt,"
        + " JSON_ARRAYAGG(rot.role_name) as roleNames,"
        + " JSON_ARRAYAGG(rot.role_id) as roleIds"
        + " FROM ("
        + " SELECT ut.user_id FROM "
        + USER_TABLE_NAME
        + " ut JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mt ON ut.metalake_id = mt.metalake_id"
        + " WHERE mt.metalake_name = #{metalakeName}"
        + " AND ut.deleted_at = 0 AND mt.deleted_at = 0"
        + " ORDER BY ut.user_id ASC LIMIT #{limit} OFFSET #{offset}"
        + " ) paginated"
        + " JOIN "
        + USER_TABLE_NAME
        + " ut ON ut.user_id = paginated.user_id"
        + " LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + USER_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rt ON rt.user_id = ut.user_id"
        + " LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rot ON rot.role_id = rt.role_id"
        + " GROUP BY ut.user_id"
        + " ORDER BY ut.user_id ASC";
  }
>>>>>>> 0dcc2ec16 ([#12841] refactor(core): Remove external_id and enabled from user and group metadata (#12842))
}
