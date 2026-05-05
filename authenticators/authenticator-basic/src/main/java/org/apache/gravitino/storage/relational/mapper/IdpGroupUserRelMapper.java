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
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
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
public interface IdpGroupUserRelMapper {
  String IDP_USER_TABLE_NAME = "idp_user_meta";
  String IDP_GROUP_TABLE_NAME = "idp_group_meta";
  String IDP_GROUP_USER_REL_TABLE_NAME = "idp_group_user_rel";

  @SelectProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "selectGroupNamesByUserId")
  List<String> selectGroupNamesByUserId(@Param("userId") Long userId);

  @SelectProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "selectUserNamesByGroupId")
  List<String> selectUserNamesByGroupId(@Param("groupId") Long groupId);

  @SelectProvider(type = IdpGroupUserRelSQLProviderFactory.class, method = "selectRelatedUserIds")
  List<Long> selectRelatedUserIds(
      @Param("groupId") Long groupId, @Param("userIds") List<Long> userIds);

  @InsertProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "batchInsertLocalGroupUsers")
  void batchInsertLocalGroupUsers(@Param("relations") List<IdpGroupUserRelPO> relations);

  @UpdateProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "softDeleteLocalGroupUsers")
  void softDeleteLocalGroupUsers(
      @Param("groupId") Long groupId,
      @Param("userIds") List<Long> userIds,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo);

  @UpdateProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "softDeleteGroupUsersByUserId")
  void softDeleteGroupUsersByUserId(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo);

  @UpdateProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "softDeleteGroupUsersByGroupId")
  void softDeleteGroupUsersByGroupId(
      @Param("groupId") Long groupId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo);

  @DeleteProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "truncateLocalGroupUserRel")
  Integer truncateLocalGroupUserRel();
}
