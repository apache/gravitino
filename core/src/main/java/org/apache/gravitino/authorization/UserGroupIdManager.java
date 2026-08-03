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
package org.apache.gravitino.authorization;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages user and group operations keyed by Gravitino-assigned id within a metalake. */
class UserGroupIdManager extends UserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(UserGroupIdManager.class);

  /**
   * Creates a {@link UserGroupIdManager} instance.
   *
   * @param store the entity store
   * @param idGenerator the id generator
   */
  UserGroupIdManager(EntityStore store, IdGenerator idGenerator) {
    super(store, idGenerator);
  }

  boolean removeUserById(String metalake, long userId) {
    try {
      return store
          .idOperations()
          .deleteById(AuthorizationUtils.ofUserId(metalake, userId), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user with id {} in the metalake {} failed due to storage issues",
          userId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  User getUserById(String metalake, long userId) throws NoSuchUserException {
    try {
      return store
          .idOperations()
          .getById(
              AuthorizationUtils.ofUserId(metalake, userId),
              Entity.EntityType.USER,
              UserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("User with id {} does not exist in the metalake {}", userId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_ID_DOES_NOT_EXIST_MSG, userId, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user with id {} failed due to storage issues", userId, ioe);
      throw new RuntimeException(ioe);
    }
  }

  User alterUserById(String metalake, long userId, UserChange... changes)
      throws NoSuchUserException {
    Preconditions.checkArgument(
        changes != null && changes.length > 0, "User changes cannot be empty");
    try {
      return store
          .idOperations()
          .updateById(
              AuthorizationUtils.ofUserId(metalake, userId),
              Entity.EntityType.USER,
              UserEntity.class,
              user -> applyChanges(user, changes));
    } catch (NoSuchEntityException e) {
      LOG.warn("User with id {} does not exist in the metalake {}", userId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_ID_DOES_NOT_EXIST_MSG, userId, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Altering user with id {} in the metalake {} failed due to storage issues",
          userId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private static UserEntity applyChanges(UserEntity user, UserChange... changes) {
    String externalId = user.externalId();
    boolean enabled = user.enabled();
    for (UserChange change : changes) {
      if (change instanceof UserChange.UpdateEnabled) {
        enabled = ((UserChange.UpdateEnabled) change).enabled();
      } else if (change instanceof UserChange.UpdateExternalId) {
        externalId = ((UserChange.UpdateExternalId) change).getNewExternalId();
      } else {
        throw new IllegalArgumentException("Unsupported user change: " + change);
      }
    }
    return UserEntity.builder()
        .withId(user.id())
        .withName(user.name())
        .withNamespace(user.namespace())
        .withExternalId(externalId)
        .withEnabled(enabled)
        .withRoleNames(user.roleNames())
        .withRoleIds(user.roleIds())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(user.auditInfo().creator())
                .withCreateTime(user.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }
}
