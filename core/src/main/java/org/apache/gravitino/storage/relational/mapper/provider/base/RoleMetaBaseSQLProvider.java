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

import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.GROUP_ROLE_RELATION_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.RoleMetaMapper.USER_ROLE_RELATION_TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.ibatis.annotations.Param;

public class RoleMetaBaseSQLProvider {

  public String selectRoleMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String roleName) {
    return "SELECT role_id as roleId, role_name as roleName,"
        + " metalake_id as metalakeId, properties as properties,"
        + " audit_info as auditInfo, current_version as currentVersion,"
        + " last_version as lastVersion, deleted_at as deletedAt"
        + " FROM "
        + ROLE_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND role_name = #{roleName}"
        + " AND deleted_at = 0";
  }

  public String selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name) {
    return "SELECT role_id as roleId FROM "
        + ROLE_TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND role_name = #{roleName}"
        + " AND deleted_at = 0";
  }

  public String listRolesByUserId(@Param("userId") Long userId) {
    return "SELECT ro.role_id as roleId, ro.role_name as roleName,"
        + " ro.metalake_id as metalakeId, ro.properties as properties,"
        + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
        + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
        + " FROM "
        + ROLE_TABLE_NAME
        + " ro JOIN "
        + USER_ROLE_RELATION_TABLE_NAME
        + " re ON ro.role_id = re.role_id"
        + " WHERE re.user_id = #{userId}"
        + " AND ro.deleted_at = 0 AND re.deleted_at = 0";
  }

  public String listRolesByGroupId(Long groupId) {
    return "SELECT ro.role_id as roleId, ro.role_name as roleName,"
        + " ro.metalake_id as metalakeId, ro.properties as properties,"
        + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
        + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
        + " FROM "
        + ROLE_TABLE_NAME
        + " ro JOIN "
        + GROUP_ROLE_RELATION_TABLE_NAME
        + " ge ON ro.role_id = ge.role_id"
        + " WHERE ge.group_id = #{groupId}"
        + " AND ro.deleted_at = 0 AND ge.deleted_at = 0";
  }

  public String listRolesByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return "SELECT DISTINCT ro.role_id as roleId, ro.role_name as roleName,"
        + " ro.metalake_id as metalakeId, ro.properties as properties,"
        + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
        + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
        + " FROM "
        + ROLE_TABLE_NAME
        + " ro JOIN "
        + SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME
        + " se ON ro.role_id = se.role_id"
        + " WHERE se.metadata_object_id = #{metadataObjectId}"
        + " AND se.type = #{metadataObjectType}"
        + " AND ro.deleted_at = 0 AND se.deleted_at = 0";
  }

  public String listRolePOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT rt.role_id as roleId, rt.role_name as roleName,"
        + " rt.metalake_id as metalakeId, rt.properties as properties,"
        + " rt.audit_info as auditInfo, rt.current_version as currentVersion,"
        + " rt.last_version as lastVersion, rt.deleted_at as deletedAt"
        + " FROM "
        + ROLE_TABLE_NAME
        + " rt JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mt ON rt.metalake_id = mt.metalake_id"
        + " WHERE mt.metalake_name = #{metalakeName}"
        + " AND rt.deleted_at = 0 AND mt.deleted_at = 0";
  }

  public String insertRoleMeta(@Param("roleMeta") RolePO rolePO) {
    return "INSERT INTO "
        + ROLE_TABLE_NAME
        + " (role_id, role_name,"
        + " metalake_id, properties,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{roleMeta.roleId},"
        + " #{roleMeta.roleName},"
        + " #{roleMeta.metalakeId},"
        + " #{roleMeta.properties},"
        + " #{roleMeta.auditInfo},"
        + " #{roleMeta.currentVersion},"
        + " #{roleMeta.lastVersion},"
        + " #{roleMeta.deletedAt}"
        + " )";
  }

  public String insertRoleMetaOnDuplicateKeyUpdate(@Param("roleMeta") RolePO rolePO) {
    return "INSERT INTO "
        + ROLE_TABLE_NAME
        + " (role_id, role_name,"
        + " metalake_id, properties,"
        + " audit_info, current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{roleMeta.roleId},"
        + " #{roleMeta.roleName},"
        + " #{roleMeta.metalakeId},"
        + " #{roleMeta.properties},"
        + " #{roleMeta.auditInfo},"
        + " #{roleMeta.currentVersion},"
        + " #{roleMeta.lastVersion},"
        + " #{roleMeta.deletedAt}"
        + " ) ON DUPLICATE KEY UPDATE"
        + " role_name = #{roleMeta.roleName},"
        + " metalake_id = #{roleMeta.metalakeId},"
        + " properties = #{roleMeta.properties},"
        + " audit_info = #{roleMeta.auditInfo},"
        + " current_version = #{roleMeta.currentVersion},"
        + " last_version = #{roleMeta.lastVersion},"
        + " deleted_at = #{roleMeta.deletedAt}";
  }

  public String updateRoleMeta(
      @Param("newRoleMeta") RolePO newRolePO, @Param("oldRoleMeta") RolePO oldRolePO) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET role_name = #{newRoleMeta.roleName},"
        + " metalake_id = #{newRoleMeta.metalakeId},"
        + " properties = #{newRoleMeta.properties},"
        + " audit_info = #{newRoleMeta.auditInfo},"
        + " current_version = #{newRoleMeta.currentVersion},"
        + " last_version = #{newRoleMeta.lastVersion},"
        + " deleted_at = #{newRoleMeta.deletedAt}"
        + " WHERE role_id = #{oldRoleMeta.roleId}"
        + " AND role_name = #{oldRoleMeta.roleName}"
        + " AND metalake_id = #{oldRoleMeta.metalakeId}"
        + " AND current_version = #{oldRoleMeta.currentVersion}"
        + " AND last_version = #{oldRoleMeta.lastVersion}"
        + " AND deleted_at = 0";
  }

  public String softDeleteRoleMetaByRoleId(Long roleId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  public String softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + ROLE_TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + ROLE_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }
}
