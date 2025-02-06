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

import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.GroupMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.ibatis.annotations.Param;

public class GroupMetaPostgreSQLProvider extends GroupMetaBaseSQLProvider {
  @Override
  public String softDeleteGroupMetaByGroupId(Long groupId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteGroupMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertGroupMetaOnDuplicateKeyUpdate(GroupPO groupPO) {
    return "INSERT INTO "
        + GROUP_TABLE_NAME
        + "(group_id, group_name,"
        + "metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES("
        + " #{groupMeta.groupId},"
        + " #{groupMeta.groupName},"
        + " #{groupMeta.metalakeId},"
        + " #{groupMeta.auditInfo},"
        + " #{groupMeta.currentVersion},"
        + " #{groupMeta.lastVersion},"
        + " #{groupMeta.deletedAt}"
        + " )"
        + " ON CONFLICT(group_id) DO UPDATE SET"
        + " group_name = #{groupMeta.groupName},"
        + " metalake_id = #{groupMeta.metalakeId},"
        + " audit_info = #{groupMeta.auditInfo},"
        + " current_version = #{groupMeta.currentVersion},"
        + " last_version = #{groupMeta.lastVersion},"
        + " deleted_at = #{groupMeta.deletedAt}";
  }

  @Override
  public String listExtendedGroupPOsByMetalakeId(Long metalakeId) {
    return "SELECT gt.group_id as groupId, gt.group_name as groupName,"
        + " gt.metalake_id as metalakeId,"
        + " gt.audit_info as auditInfo,"
        + " gt.current_version as currentVersion, gt.last_version as lastVersion,"
        + " gt.deleted_at as deletedAt,"
        + " JSON_AGG(rot.role_name) as roleNames,"
        + " JSON_AGG(rot.role_id) as roleIds"
        + " FROM "
        + GROUP_TABLE_NAME
        + " gt LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rt ON rt.group_id = gt.group_id"
        + " LEFT OUTER JOIN ("
        + " SELECT * FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at = 0)"
        + " AS rot ON rot.role_id = rt.role_id"
        + " WHERE "
        + " gt.deleted_at = 0 AND"
        + " gt.metalake_id = #{metalakeId}"
        + " GROUP BY gt.group_id";
  }

  @Override
  public String deleteGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + GROUP_TABLE_NAME
        + " WHERE group_id IN (SELECT group_id FROM "
        + GROUP_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
