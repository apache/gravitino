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
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.RolePO;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.po.auth.RoleUpdatedAt;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Many;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

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

  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "selectRoleMetaByMetalakeIdAndName")
  RolePO selectRoleMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String roleName);

  /**
   * Returns an active role by ID and holds its lock for the current transaction.
   *
   * <p>The lock is shared on MySQL/PostgreSQL and exclusive on H2.
   *
   * @return the active role, or null if it does not exist
   */
  @Nullable
  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "selectRoleMetaByIdForShare")
  RolePO selectRoleMetaByIdForShare(@Param("roleId") Long roleId);

  /** Returns and locks an active role by ID for the current transaction. */
  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "selectRoleMetaByIdForUpdate")
  RolePO selectRoleMetaByIdForUpdate(@Param("roleId") Long roleId);

  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "selectRoleIdByMetalakeIdAndName")
  Long selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "listRolesByUserId")
  List<RolePO> listRolesByUserId(@Param("userId") Long userId);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "listRolesByGroupId")
  List<RolePO> listRolesByGroupId(@Param("groupId") Long groupId);

  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "listRolesByMetadataObjectIdAndType")
  List<RolePO> listRolesByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "listRolePOsByMetalake")
  List<RolePO> listRolePOsByMetalake(@Param("metalakeName") String metalakeName);

  @InsertProvider(type = RoleMetaSQLProviderFactory.class, method = "insertRoleMeta")
  void insertRoleMeta(@Param("roleMeta") RolePO rolePO);

  @InsertProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "insertRoleMetaOnDuplicateKeyUpdate")
  void insertRoleMetaOnDuplicateKeyUpdate(@Param("roleMeta") RolePO rolePO);

  @UpdateProvider(type = RoleMetaSQLProviderFactory.class, method = "updateRoleMeta")
  Integer updateRoleMeta(
      @Param("newRoleMeta") RolePO newRolePO, @Param("oldRoleMeta") RolePO oldRolePO);

  /**
   * Soft-deletes an active role only when its OCC version still matches.
   *
   * @return the number of deleted rows
   */
  @UpdateProvider(type = RoleMetaSQLProviderFactory.class, method = "softDeleteRoleMetaByRoleId")
  Integer softDeleteRoleMetaByRoleId(
      @Param("roleId") Long roleId, @Param("currentVersion") Long currentVersion);

  @UpdateProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "softDeleteRoleMetasByMetalakeId")
  void softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "deleteRoleMetasByLegacyTimeline")
  Integer deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @UpdateProvider(type = RoleMetaSQLProviderFactory.class, method = "touchRoleUpdatedAt")
  void touchRoleUpdatedAt(@Param("roleId") long roleId);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "batchGetRoleUpdatedAt")
  List<RoleUpdatedAt> batchGetRoleUpdatedAt(@Param("roleIds") List<Long> roleIds);
  /**
   * Defines the nested result mapping for single-statement role reads.
   *
   * @return unused; this declaration is never executed
   */
  @Results(
      id = "rolePrivilegeSnapshot",
      value = {
        @Result(property = "id", column = "relationId", id = true),
        @Result(property = "roleId", column = "roleId"),
        @Result(property = "metadataObjectId", column = "metadataObjectId"),
        @Result(property = "type", column = "type"),
        @Result(property = "privilegeNames", column = "privilegeNames"),
        @Result(property = "privilegeConditions", column = "privilegeConditions"),
        @Result(property = "currentVersion", column = "currentVersion"),
        @Result(property = "lastVersion", column = "lastVersion"),
        @Result(property = "deletedAt", column = "deletedAt")
      })
  @Select("SELECT 1")
  SecurableObjectPO rolePrivilegeSnapshot();

  /**
   * Reads a role and its live privileges from one statement snapshot.
   *
   * @param metalakeId the parent metalake ID
   * @param roleName the role name
   * @return the role snapshot, or null when it does not exist
   */
  @Results(
      id = "roleWithPrivileges",
      value = {
        @Result(property = "roleId", column = "roleId", id = true),
        @Result(property = "roleName", column = "roleName"),
        @Result(property = "metalakeId", column = "metalakeId"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "auditInfo", column = "auditInfo"),
        @Result(property = "currentVersion", column = "currentVersion"),
        @Result(property = "lastVersion", column = "lastVersion"),
        @Result(property = "deletedAt", column = "deletedAt"),
        @Result(
            property = "securableObjectPOs",
            javaType = List.class,
            many = @Many(resultMap = "rolePrivilegeSnapshot", columnPrefix = "priv_"))
      })
  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "selectRoleWithPrivilegesByMetalakeIdAndName")
  @Nullable
  RolePO selectRoleWithPrivilegesByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String roleName);

  /**
   * Lists matching roles and all their privileges from one statement snapshot.
   *
   * @param metadataObjectId the metadata object ID
   * @param metadataObjectType the metadata object type
   * @return the role snapshots
   */
  @ResultMap("roleWithPrivileges")
  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "listRolesWithPrivilegesByMetadataObjectIdAndType")
  List<RolePO> listRolesWithPrivilegesByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);
  /**
   * Defines the role metadata mapping shared by principal snapshot reads.
   *
   * @return unused; this declaration is never executed
   */
  @Results(
      id = "membershipRoleSnapshot",
      value = {
        @Result(property = "roleId", column = "roleId", id = true),
        @Result(property = "roleName", column = "roleName"),
        @Result(property = "metalakeId", column = "metalakeId"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "auditInfo", column = "auditInfo"),
        @Result(property = "currentVersion", column = "currentVersion"),
        @Result(property = "lastVersion", column = "lastVersion"),
        @Result(property = "deletedAt", column = "deletedAt")
      })
  @Select("SELECT 1")
  RolePO membershipRoleSnapshot();
}
