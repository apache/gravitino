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
import org.apache.gravitino.storage.relational.po.GroupRoleRelPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface GroupRoleRelMapper {
  String GROUP_TABLE_NAME = "group_meta";
  String GROUP_ROLE_RELATION_TABLE_NAME = "group_role_rel";

  @InsertProvider(type = GroupRoleRelSQLProviderFactory.class, method = "batchInsertGroupRoleRel")
  void batchInsertGroupRoleRel(@Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS);

  @InsertProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "batchInsertGroupRoleRelOnDuplicateKeyUpdate")
  void batchInsertGroupRoleRelOnDuplicateKeyUpdate(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS);

  @UpdateProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "softDeleteGroupRoleRelByGroupId")
  void softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId);

  @UpdateProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "softDeleteGroupRoleRelByGroupAndRoles")
  void softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds);

  @UpdateProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "softDeleteGroupRoleRelByMetalakeId")
  void softDeleteGroupRoleRelByMetalakeId(Long metalakeId);

  @UpdateProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "softDeleteGroupRoleRelByRoleId")
  void softDeleteGroupRoleRelByRoleId(Long roleId);

  @UpdateProvider(
      type = GroupRoleRelSQLProviderFactory.class,
      method = "deleteGroupRoleRelMetasByLegacyTimeline")
  Integer deleteGroupRoleRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
