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

import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.GroupMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class GroupMetaH2Provider extends GroupMetaBaseSQLProvider {
  @Override
  public String listExtendedGroupPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT gt.group_id as groupId, gt.group_name as groupName,"
        + " gt.metalake_id as metalakeId,"
        + " gt.audit_info as auditInfo,"
        + " gt.current_version as currentVersion, gt.last_version as lastVersion,"
        + " gt.deleted_at as deletedAt,"
        + " '[' || GROUP_CONCAT('\"' || rot.role_name || '\"') || ']' as roleNames,"
        + " '[' || GROUP_CONCAT('\"' || rot.role_id || '\"') || ']' as roleIds"
        + " FROM "
        + GROUP_TABLE_NAME
        + " gt LEFT OUTER JOIN "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " rt ON rt.group_id = gt.group_id"
        + " LEFT OUTER JOIN "
        + ROLE_TABLE_NAME
        + " rot ON rot.role_id = rt.role_id"
        + " WHERE "
        + " gt.deleted_at = 0 AND"
        + " (rot.deleted_at = 0 OR rot.deleted_at is NULL) AND"
        + " (rt.deleted_at = 0 OR rt.deleted_at is NULL) AND gt.metalake_id = #{metalakeId}"
        + " GROUP BY gt.group_id";
  }
}
