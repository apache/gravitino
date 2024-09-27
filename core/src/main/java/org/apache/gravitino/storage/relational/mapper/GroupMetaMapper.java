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
import org.apache.gravitino.storage.relational.po.ExtendedGroupPO;
import org.apache.gravitino.storage.relational.po.GroupPO;
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
public interface GroupMetaMapper {
  String GROUP_TABLE_NAME = "group_meta";
  String GROUP_ROLE_RELATION_TABLE_NAME = "group_role_rel";

  @SelectProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "selectGroupIdBySchemaIdAndName")
  Long selectGroupIdBySchemaIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name);

  @SelectProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "selectGroupMetaByMetalakeIdAndName")
  GroupPO selectGroupMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("groupName") String name);

  @SelectProvider(type = GroupMetaSQLProviderFactory.class, method = "listGroupPOsByMetalake")
  List<GroupPO> listGroupPOsByMetalake(@Param("metalakeName") String metalakeName);

  @SelectProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "listExtendedGroupPOsByMetalakeId")
  List<ExtendedGroupPO> listExtendedGroupPOsByMetalakeId(Long metalakeId);

  @InsertProvider(type = GroupMetaSQLProviderFactory.class, method = "insertGroupMeta")
  void insertGroupMeta(@Param("groupMeta") GroupPO groupPO);

  @InsertProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "insertGroupMetaOnDuplicateKeyUpdate")
  void insertGroupMetaOnDuplicateKeyUpdate(@Param("groupMeta") GroupPO groupPO);

  @UpdateProvider(type = GroupMetaSQLProviderFactory.class, method = "softDeleteGroupMetaByGroupId")
  void softDeleteGroupMetaByGroupId(@Param("groupId") Long groupId);

  @UpdateProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "softDeleteGroupMetasByMetalakeId")
  void softDeleteGroupMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(type = GroupMetaSQLProviderFactory.class, method = "updateGroupMeta")
  Integer updateGroupMeta(
      @Param("newGroupMeta") GroupPO newGroupPO, @Param("oldGroupMeta") GroupPO oldGroupPO);

  @SelectProvider(type = GroupMetaSQLProviderFactory.class, method = "listGroupsByRoleId")
  List<GroupPO> listGroupsByRoleId(@Param("roleId") Long roleId);

  @DeleteProvider(
      type = GroupMetaSQLProviderFactory.class,
      method = "deleteGroupMetasByLegacyTimeline")
  Integer deleteGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
