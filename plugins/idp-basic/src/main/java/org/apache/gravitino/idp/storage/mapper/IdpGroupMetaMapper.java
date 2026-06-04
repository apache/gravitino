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

import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpGroupWithUsersPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis mapper for built-in IdP group metadata operations.
 *
 * <p>This interface defines the SQL statements MyBatis executes for the built-in IdP group metadata
 * store. The SQLs are provided through {@code *Provider} annotations on this mapper interface. See
 * the <a href="https://mybatis.org/mybatis-3/getting-started.html">MyBatis getting started
 * guide</a>.
 */
public interface IdpGroupMetaMapper {
  String IDP_GROUP_TABLE_NAME = "idp_group_meta";

  @SelectProvider(type = IdpGroupMetaSQLProviderFactory.class, method = "selectIdpGroup")
  IdpGroupPO selectIdpGroup(@Param("groupName") String groupName);

  @SelectProvider(type = IdpGroupMetaSQLProviderFactory.class, method = "selectIdpGroupWithUsers")
  IdpGroupWithUsersPO selectIdpGroupWithUsers(@Param("groupName") String groupName);

  @InsertProvider(type = IdpGroupMetaSQLProviderFactory.class, method = "insertIdpGroup")
  void insertIdpGroup(@Param("groupMeta") IdpGroupPO groupPO);

  @UpdateProvider(type = IdpGroupMetaSQLProviderFactory.class, method = "softDeleteIdpGroup")
  Integer softDeleteIdpGroup(@Param("groupName") String groupName);

  @DeleteProvider(
      type = IdpGroupMetaSQLProviderFactory.class,
      method = "deleteIdpGroupMetasByLegacyTimeline")
  Integer deleteIdpGroupMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
