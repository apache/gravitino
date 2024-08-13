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
import org.apache.gravitino.storage.relational.po.GroupPO;
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
public interface GroupMetaMapper {
  String GROUP_TABLE_NAME = "group_meta";
  String GROUP_ROLE_RELATION_TABLE_NAME = "group_role_rel";

  @Select(
      "SELECT group_id as groupId FROM "
          + GROUP_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND group_name = #{groupName}"
          + " AND deleted_at = 0")
  Long selectGroupIdBySchemaIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name);

  @Select(
      "SELECT group_id as groupId, group_name as groupName,"
          + " metalake_id as metalakeId,"
          + " audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + GROUP_TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND group_name = #{groupName}"
          + " AND deleted_at = 0")
  GroupPO selectGroupMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name);

  @Insert(
      "INSERT INTO "
          + GROUP_TABLE_NAME
          + "(group_id, group_name,"
          + " metalake_id, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{groupMeta.groupId},"
          + " #{groupMeta.groupName},"
          + " #{groupMeta.metalakeId},"
          + " #{groupMeta.auditInfo},"
          + " #{groupMeta.currentVersion},"
          + " #{groupMeta.lastVersion},"
          + " #{groupMeta.deletedAt}"
          + " )")
  void insertGroupMeta(@Param("groupMeta") GroupPO groupPO);

  @Insert(
      "INSERT INTO "
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
          + " ON DUPLICATE KEY UPDATE"
          + " group_name = #{groupMeta.groupName},"
          + " metalake_id = #{groupMeta.metalakeId},"
          + " audit_info = #{groupMeta.auditInfo},"
          + " current_version = #{groupMeta.currentVersion},"
          + " last_version = #{groupMeta.lastVersion},"
          + " deleted_at = #{groupMeta.deletedAt}")
  void insertGroupMetaOnDuplicateKeyUpdate(@Param("groupMeta") GroupPO groupPO);

  @Update(
      "UPDATE "
          + GROUP_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE group_id = #{groupId} AND deleted_at = 0")
  void softDeleteGroupMetaByGroupId(@Param("groupId") Long groupId);

  @Update(
      "UPDATE "
          + GROUP_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  void softDeleteGroupMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
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
          + " AND deleted_at = 0")
  Integer updateGroupMeta(
      @Param("newGroupMeta") GroupPO newGroupPO, @Param("oldGroupMeta") GroupPO oldGroupPO);

  @Select(
      "SELECT gr.group_id as groupId, gr.group_name as groupName,"
          + " gr.metalake_id as metalakeId,"
          + " gr.audit_info as auditInfo, gr.current_version as currentVersion,"
          + " gr.last_version as lastVersion, gr.deleted_at as deletedAt"
          + " FROM "
          + GROUP_TABLE_NAME
          + " gr JOIN "
          + GROUP_ROLE_RELATION_TABLE_NAME
          + " re ON gr.group_id = re.group_id"
          + " WHERE re.role_id = #{roleId}"
          + " AND gr.deleted_at = 0 AND re.deleted_at = 0")
  List<GroupPO> listGroupsByRoleId(@Param("roleId") Long roleId);

  @Delete(
      "DELETE FROM "
          + GROUP_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
