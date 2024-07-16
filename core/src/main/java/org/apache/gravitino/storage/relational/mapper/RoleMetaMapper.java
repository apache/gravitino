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

package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface RoleMetaMapper {
  String ROLE_TABLE_NAME = "role_meta";
  String USER_ROLE_RELATION_TABLE_NAME = "user_role_rel";
  String GROUP_ROLE_RELATION_TABLE_NAME = "group_role_rel";

  @Select(
      "SELECT role_id as roleId, role_name as roleName,"
          + " metalake_id as metalakeId, properties as properties,"
          + " audit_info as auditInfo, current_version as currentVersion,"
          + " last_version as lastVersion, deleted_at as deletedAt"
          + " FROM "
          + ROLE_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND role_name = #{roleName}"
          + " AND deleted_at = 0")
  RolePO selectRoleMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String roleName);

  @Select(
      "SELECT role_id as roleId FROM "
          + ROLE_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND role_name = #{roleName}"
          + " AND deleted_at = 0")
  Long selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name);

  @Select(
      "SELECT ro.role_id as roleId, ro.role_name as roleName,"
          + " ro.metalake_id as metalakeId, ro.properties as properties,"
          + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
          + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
          + " FROM "
          + ROLE_TABLE_NAME
          + " ro JOIN "
          + USER_ROLE_RELATION_TABLE_NAME
          + " re ON ro.role_id = re.role_id"
          + " WHERE re.user_id = #{userId}"
          + " AND ro.deleted_at = 0 AND re.deleted_at = 0")
  List<RolePO> listRolesByUserId(@Param("userId") Long userId);

  @Select(
      "SELECT ro.role_id as roleId, ro.role_name as roleName,"
          + " ro.metalake_id as metalakeId, ro.properties as properties,"
          + " ro.audit_info as auditInfo, ro.current_version as currentVersion,"
          + " ro.last_version as lastVersion, ro.deleted_at as deletedAt"
          + " FROM "
          + ROLE_TABLE_NAME
          + " ro JOIN "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " ge ON ro.role_id = ge.role_id"
          + " WHERE ge.group_id = #{groupId}"
          + " AND ro.deleted_at = 0 AND ge.deleted_at = 0")
  List<RolePO> listRolesByGroupId(Long groupId);

  @Insert(
      "INSERT INTO "
          + ROLE_TABLE_NAME
          + "(role_id, role_name,"
          + " metalake_id, properties,"
          + " audit_info, current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{roleMeta.roleId},"
          + " #{roleMeta.roleName},"
          + " #{roleMeta.metalakeId},"
          + " #{roleMeta.properties},"
          + " #{roleMeta.auditInfo},"
          + " #{roleMeta.currentVersion},"
          + " #{roleMeta.lastVersion},"
          + " #{roleMeta.deletedAt}"
          + " )")
  void insertRoleMeta(@Param("roleMeta") RolePO rolePO);

  @Insert(
      "INSERT INTO "
          + ROLE_TABLE_NAME
          + "(role_id, role_name,"
          + " metalake_id, properties,"
          + " audit_info, current_version, last_version, deleted_at)"
          + " VALUES("
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
          + " deleted_at = #{roleMeta.deletedAt}")
  void insertRoleMetaOnDuplicateKeyUpdate(@Param("roleMeta") RolePO rolePO);

  @Update(
      "UPDATE "
          + ROLE_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE role_id = #{roleId} AND deleted_at = 0")
  void softDeleteRoleMetaByRoleId(Long roleId);

  @Update(
      "UPDATE "
          + ROLE_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  void softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Delete(
      "DELETE FROM "
          + ROLE_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
