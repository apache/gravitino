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
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.po.IdpUserWithGroupsPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis mapper for built-in IdP user metadata operations.
 *
 * <p>This interface defines the SQL statements MyBatis executes for the built-in IdP user metadata
 * store. The SQLs are provided through {@code *Provider} annotations on this mapper interface. See
 * the <a href="https://mybatis.org/mybatis-3/getting-started.html">MyBatis getting started
 * guide</a>.
 */
public interface IdpUserMetaMapper {
  String IDP_USER_TABLE_NAME = "idp_user_meta";

  @SelectProvider(type = IdpUserMetaSQLProviderFactory.class, method = "selectIdpUser")
  IdpUserPO selectIdpUser(@Param("username") String username);

  @SelectProvider(type = IdpUserMetaSQLProviderFactory.class, method = "selectIdpUserWithGroups")
  IdpUserWithGroupsPO selectIdpUserWithGroups(@Param("username") String username);

  @SelectProvider(type = IdpUserMetaSQLProviderFactory.class, method = "selectIdpUsersByUsernames")
  List<IdpUserPO> selectIdpUsersByUsernames(@Param("usernames") List<String> usernames);

  @InsertProvider(type = IdpUserMetaSQLProviderFactory.class, method = "insertIdpUser")
  void insertIdpUser(@Param("userMeta") IdpUserPO userPO);

  @UpdateProvider(type = IdpUserMetaSQLProviderFactory.class, method = "updateIdpUserPassword")
  Integer updateIdpUserPassword(
      @Param("username") String username, @Param("passwordHash") String passwordHash);

  @UpdateProvider(type = IdpUserMetaSQLProviderFactory.class, method = "softDeleteIdpUser")
  Integer softDeleteIdpUser(@Param("username") String username);

  @DeleteProvider(
      type = IdpUserMetaSQLProviderFactory.class,
      method = "deleteIdpUserMetasByLegacyTimeline")
  Integer deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
