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
import org.apache.gravitino.idp.storage.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.gravitino.idp.storage.mapper.provider.h2.IdpUserMetaH2Provider;
import org.apache.gravitino.idp.storage.mapper.provider.postgresql.IdpUserMetaPostgreSQLProvider;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaSQLProviderFactory
    extends IdpBaseSQLProviderFactory<IdpUserMetaBaseSQLProvider> {
  public IdpUserMetaSQLProviderFactory() {
    super(
        "IdP user SQL provider",
        new IdpUserMetaBaseSQLProvider(),
        new IdpUserMetaH2Provider(),
        new IdpUserMetaPostgreSQLProvider());
  }

  public String selectIdpUser(@Param("username") String username) {
    return currentProvider().selectIdpUser(username);
  }

  public String selectIdpUsers(@Param("usernames") List<String> usernames) {
    return currentProvider().selectIdpUsers(usernames);
  }

  public String insertIdpUser(@Param("userMeta") IdpUserPO userPO) {
    return currentProvider().insertIdpUser(userPO);
  }

  public String updateIdpUserPassword(
      @Param("userId") Long userId, @Param("passwordHash") String passwordHash) {
    return currentProvider().updateIdpUserPassword(userId, passwordHash);
  }

  public String softDeleteIdpUser(@Param("userId") Long userId) {
    return currentProvider().softDeleteIdpUser(userId);
  }

  public String deleteIdpUserMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return currentProvider().deleteIdpUserMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
