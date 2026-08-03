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

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.IdGenerator;
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

  User enableUserById(String metalake, long userId) throws NoSuchUserException {
    return updateEnabledById(metalake, userId, true);
  }

  User disableUserById(String metalake, long userId) throws NoSuchUserException {
    return updateEnabledById(metalake, userId, false);
  }

  User updateUserExternalId(String metalake, long userId, String newExternalId)
      throws NoSuchUserException {
    try {
      return store
          .idOperations()
          .updateById(
              AuthorizationUtils.ofUserId(metalake, userId),
              Entity.EntityType.USER,
              UserEntity.class,
              user ->
                  UserEntity.builder()
                      .withId(user.id())
                      .withName(user.name())
                      .withNamespace(user.namespace())
                      .withExternalId(newExternalId)
                      .withEnabled(user.enabled())
                      .withRoleNames(user.roleNames())
                      .withRoleIds(user.roleIds())
                      .withAuditInfo(user.auditInfo())
                      .build());
    } catch (NoSuchEntityException e) {
      LOG.warn("User with id {} does not exist in the metalake {}", userId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_ID_DOES_NOT_EXIST_MSG, userId, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Updating external id for user with id {} in the metalake {} failed due to storage"
              + " issues",
          userId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private User updateEnabledById(String metalake, long userId, boolean enabled)
      throws NoSuchUserException {
    try {
      return store
          .idOperations()
          .updateById(
              AuthorizationUtils.ofUserId(metalake, userId),
              Entity.EntityType.USER,
              UserEntity.class,
              user ->
                  UserEntity.builder()
                      .withId(user.id())
                      .withName(user.name())
                      .withNamespace(user.namespace())
                      .withExternalId(user.externalId())
                      .withEnabled(enabled)
                      .withRoleNames(user.roleNames())
                      .withRoleIds(user.roleIds())
                      .withAuditInfo(user.auditInfo())
                      .build());
    } catch (NoSuchEntityException e) {
      LOG.warn("User with id {} does not exist in the metalake {}", userId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_ID_DOES_NOT_EXIST_MSG, userId, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Updating enabled state for user with id {} in the metalake {} failed due to storage"
              + " issues",
          userId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }
}
