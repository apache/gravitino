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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import static org.apache.gravitino.storage.relational.mapper.GroupMetaMapper.GROUP_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.ibatis.annotations.Param;

public class GroupMetaBaseSQLProvider {

  public String selectGroupIdBySchemaIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name) {
    return "SELECT group_id as groupId FROM "
        + GROUP_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND group_name = #{groupName}"
        + " AND deleted_at = 0";
  }

  public String listGroupPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT gt.group_id as groupId, gt.group_name as groupName, gt.metalake_id as metalakeId,"
        + " gt.audit_info as auditInfo, gt.current_version as currentVersion, gt.last_version as lastVersion,"
        + " gt.deleted_at as deletedAt FROM "
        + GROUP_TABLE_NAME
        + " gt JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mt ON gt.metalake_id = mt.metalake_id WHERE mt.metalake_name = #{metalakeName}"
        + " AND gt.deleted_at = 0 AND mt.deleted_at = 0";
  }

  public String listExtendedGroupPOsByMetalakeId(Long metalakeId) {
    return "SELECT gt.group_id as groupId, gt.group_name as groupName,"
        + " gt.metalake_id as metalakeId,"
        + " gt.audit_info as auditInfo,"
        + " gt.current_version as currentVersion, gt.last_version as lastVersion,"
        + " gt.deleted_at as deletedAt,"
        + " JSON_ARRAYAGG(rot.role_name) as roleNames,"
        + " JSON_ARRAYAGG(rot.role_id) as roleIds"
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
        + " WHERE"
        + " gt.deleted_at = 0 AND"
        + " gt.metalake_id = #{metalakeId}"
        + " GROUP BY gt.group_id";
  }

  public String selectGroupMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name) {
    return "SELECT group_id as groupId, group_name as groupName,"
        + " metalake_id as metalakeId,"
        + " audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + GROUP_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND group_name = #{groupName}"
        + " AND deleted_at = 0";
  }

  public String insertGroupMeta(@Param("groupMeta") GroupPO groupPO) {
    return "INSERT INTO "
        + GROUP_TABLE_NAME
        + " (group_id, group_name,"
        + " metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{groupMeta.groupId},"
        + " #{groupMeta.groupName},"
        + " #{groupMeta.metalakeId},"
        + " #{groupMeta.auditInfo},"
        + " #{groupMeta.currentVersion},"
        + " #{groupMeta.lastVersion},"
        + " #{groupMeta.deletedAt}"
        + " )";
  }

  public String insertGroupMetaOnDuplicateKeyUpdate(@Param("groupMeta") GroupPO groupPO) {
    return "INSERT INTO "
        + GROUP_TABLE_NAME
        + " (group_id, group_name,"
        + " metalake_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{groupMeta.groupId},"
        + " #{groupMeta.groupName},"
        + " #{groupMeta.metalakeId},"
        + " #{groupMeta.auditInfo},"
        + " #{groupMeta.currentVersion},"
        + " #{groupMeta.lastVersion},"
        + " #{groupMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " group_name = #{groupMeta.groupName},"
        + " metalake_id = #{groupMeta.metalakeId},"
        + " audit_info = #{groupMeta.auditInfo},"
        + " current_version = #{groupMeta.currentVersion},"
        + " last_version = #{groupMeta.lastVersion},"
        + " deleted_at = #{groupMeta.deletedAt}";
  }

  public String softDeleteGroupMetaByGroupId(@Param("groupId") Long groupId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE group_id = #{groupId} AND deleted_at = 0";
  }

  public String softDeleteGroupMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String updateGroupMeta(
      @Param("newGroupMeta") GroupPO newGroupPO, @Param("oldGroupMeta") GroupPO oldGroupPO) {
    return "UPDATE "
        + GROUP_TABLE_NAME
        + " SET group_name = #{newGroupMeta.groupName},"
        + " metalake_id = #{newGroupMeta.metalakeId},"
        + " audit_info = #{newGroupMeta.auditInfo},"
        + " current_version = #{newGroupMeta.currentVersion},"
        + " last_version = #{newGroupMeta.lastVersion},"
        + " deleted_at = #{newGroupMeta.deletedAt}"
        + " WHERE group_id = #{oldGroupMeta.groupId}"
        + " AND group_name = #{oldGroupMeta.groupName}"
        + " AND metalake_id = #{oldGroupMeta.metalakeId}"
        + " AND audit_info = #{oldGroupMeta.auditInfo}"
        + " AND current_version = #{oldGroupMeta.currentVersion}"
        + " AND last_version = #{oldGroupMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String listGroupsByRoleId(@Param("roleId") Long roleId) {
    return "SELECT gr.group_id as groupId, gr.group_name as groupName,"
        + " gr.metalake_id as metalakeId,"
        + " gr.audit_info as auditInfo, gr.current_version as currentVersion,"
        + " gr.last_version as lastVersion, gr.deleted_at as deletedAt"
        + " FROM "
        + GROUP_TABLE_NAME
        + " gr JOIN "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " re ON gr.group_id = re.group_id"
        + " WHERE re.role_id = #{roleId}"
        + " AND gr.deleted_at = 0 AND re.deleted_at = 0";
  }

  public String deleteGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + GROUP_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
