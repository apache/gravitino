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

package org.apache.gravitino.idp.storage.mapper;

import java.util.List;
import org.apache.gravitino.idp.storage.po.IdpGroupUserRelPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis mapper for built-in IdP group-user relation operations.
 *
 * <p>This interface defines the SQL statements MyBatis executes for the built-in IdP group-user
 * relation store. The SQLs are provided through {@code *Provider} annotations on this mapper
 * interface. See the <a href="https://mybatis.org/mybatis-3/getting-started.html">MyBatis getting
 * started guide</a>.
 */
public interface IdpGroupUserRelMapper {
  String IDP_GROUP_USER_REL_TABLE_NAME = "idp_group_user_rel";

  @SelectProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "selectGroupNamesByUsername")
  List<String> selectGroupNamesByUsername(@Param("username") String username);

  @SelectProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "selectUsernamesByGroupName")
  List<String> selectUsernamesByGroupName(@Param("groupName") String groupName);

  @InsertProvider(type = IdpGroupUserRelSQLProviderFactory.class, method = "batchInsertRelations")
  void batchInsertRelations(@Param("relations") List<IdpGroupUserRelPO> relations);

  @UpdateProvider(type = IdpGroupUserRelSQLProviderFactory.class, method = "softDeleteRelations")
  Integer softDeleteRelations(@Param("groupId") Long groupId, @Param("userIds") List<Long> userIds);

  @UpdateProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "softDeleteRelationsByUsername")
  Integer softDeleteRelationsByUsername(@Param("username") String username);

  @UpdateProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "softDeleteRelationsByGroupName")
  Integer softDeleteRelationsByGroupName(@Param("groupName") String groupName);

  @DeleteProvider(
      type = IdpGroupUserRelSQLProviderFactory.class,
      method = "deleteIdpGroupUserRelMetasByLegacyTimeline")
  Integer deleteIdpGroupUserRelMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
