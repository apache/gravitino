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
import org.apache.gravitino.storage.relational.po.IdpUserPO;
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
public interface IdpUserMetaMapper {
  String IDP_USER_TABLE_NAME = "idp_user_meta";

  @SelectProvider(type = IdpUserMetaSQLProviderFactory.class, method = "selectIdpUser")
  IdpUserPO selectIdpUser(@Param("userName") String userName);

  @SelectProvider(type = IdpUserMetaSQLProviderFactory.class, method = "selectIdpUsers")
  List<IdpUserPO> selectIdpUsers(@Param("userNames") List<String> userNames);

  @InsertProvider(type = IdpUserMetaSQLProviderFactory.class, method = "insertIdpUser")
  void insertIdpUser(@Param("userMeta") IdpUserPO userPO);

  @UpdateProvider(type = IdpUserMetaSQLProviderFactory.class, method = "updateIdpUserPassword")
  Integer updateIdpUserPassword(
      @Param("userId") Long userId,
      @Param("passwordHash") String passwordHash,
      @Param("auditInfo") String auditInfo,
      @Param("currentVersion") Long currentVersion,
      @Param("newCurrentVersion") Long newCurrentVersion,
      @Param("newLastVersion") Long newLastVersion);

  @UpdateProvider(type = IdpUserMetaSQLProviderFactory.class, method = "softDeleteIdpUser")
  void softDeleteIdpUser(
      @Param("userId") Long userId,
      @Param("deletedAt") Long deletedAt,
      @Param("auditInfo") String auditInfo);

  @DeleteProvider(
      type = IdpUserMetaSQLProviderFactory.class,
      method = "deleteIdpUserMetasByLegacyTimeline")
  Integer deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
