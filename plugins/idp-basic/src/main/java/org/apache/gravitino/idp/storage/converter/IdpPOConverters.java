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
package org.apache.gravitino.idp.storage.converter;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.idp.meta.IdpGroupEntity;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.storage.relational.utils.POConverters;

/** Converts between built-in IdP persistence objects and entity objects. */
public final class IdpPOConverters {

  private IdpPOConverters() {}

  /**
   * Initializes an {@link IdpUserPO} from a built-in IdP user entity.
   *
   * @param userEntity The built-in IdP user entity.
   * @return The initialized persistence object.
   */
  public static IdpUserPO initializeIdpUserPO(IdpUserEntity userEntity) {
    Preconditions.checkArgument(
        userEntity.passwordHash() != null, "Password hash must be set when inserting IdP user");
    return IdpUserPO.builder()
        .withUserId(userEntity.id())
        .withUsername(userEntity.name())
        .withPasswordHash(userEntity.passwordHash())
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  /**
   * Initializes an {@link IdpGroupPO} from a built-in IdP group entity.
   *
   * @param groupEntity The built-in IdP group entity.
   * @return The initialized persistence object.
   */
  public static IdpGroupPO initializeIdpGroupPO(IdpGroupEntity groupEntity) {
    return IdpGroupPO.builder()
        .withGroupId(groupEntity.id())
        .withGroupName(groupEntity.name())
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  /**
   * Converts an {@link IdpUserPO} to a built-in IdP user entity.
   *
   * @param userPO The persistence object.
   * @param groupNames The group names of the user.
   * @return The built-in IdP user entity.
   */
  public static IdpUserEntity fromIdpUserPO(IdpUserPO userPO, List<String> groupNames) {
    IdpUserEntity.Builder builder =
        IdpUserEntity.builder().withId(userPO.getUserId()).withName(userPO.getUsername());
    if (groupNames != null && !groupNames.isEmpty()) {
      builder.withGroupNames(groupNames);
    }
    return builder.build();
  }

  /**
   * Converts an {@link IdpGroupPO} to a built-in IdP group entity.
   *
   * @param groupPO The persistence object.
   * @param usernames The usernames of the group.
   * @return The built-in IdP group entity.
   */
  public static IdpGroupEntity fromIdpGroupPO(IdpGroupPO groupPO, List<String> usernames) {
    IdpGroupEntity.Builder builder =
        IdpGroupEntity.builder().withId(groupPO.getGroupId()).withName(groupPO.getGroupName());
    if (usernames != null && !usernames.isEmpty()) {
      builder.withUsernames(usernames);
    }
    return builder.build();
  }

  /**
   * Converts an {@link IdpUserPO} to a built-in IdP user entity without membership fields.
   *
   * @param userPO The persistence object.
   * @return The built-in IdP user entity.
   */
  public static IdpUserEntity fromIdpUserPO(IdpUserPO userPO) {
    return fromIdpUserPO(userPO, Collections.emptyList());
  }

  /**
   * Converts an {@link IdpGroupPO} to a built-in IdP group entity without membership fields.
   *
   * @param groupPO The persistence object.
   * @return The built-in IdP group entity.
   */
  public static IdpGroupEntity fromIdpGroupPO(IdpGroupPO groupPO) {
    return fromIdpGroupPO(groupPO, Collections.emptyList());
  }
}
