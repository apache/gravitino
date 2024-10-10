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
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
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

  @SelectProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "selectRoleIdByMetalakeIdAndName")
  Long selectRoleIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("roleName") String name);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "listRolesByUserId")
  List<RolePO> listRolesByUserId(@Param("userId") Long userId);

  @SelectProvider(type = RoleMetaSQLProviderFactory.class, method = "listRolesByGroupId")
  List<RolePO> listRolesByGroupId(Long groupId);

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

  @UpdateProvider(type = RoleMetaSQLProviderFactory.class, method = "softDeleteRoleMetaByRoleId")
  void softDeleteRoleMetaByRoleId(Long roleId);

  @UpdateProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "softDeleteRoleMetasByMetalakeId")
  void softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = RoleMetaSQLProviderFactory.class,
      method = "deleteRoleMetasByLegacyTimeline")
  Integer deleteRoleMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
