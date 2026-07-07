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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages user operations keyed by external id within a metalake. */
class UserGroupExternalManager extends UserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(UserGroupExternalManager.class);

  /**
   * Creates a {@link UserGroupExternalManager} instance.
   *
   * @param store the entity store
   * @param idGenerator the id generator
   */
  UserGroupExternalManager(EntityStore store, IdGenerator idGenerator) {
    super(store, idGenerator);
  }

  User addUser(String metalake, String name, String externalId, boolean enabled)
      throws UserAlreadyExistsException {
    try {
      UserEntity userEntity =
          UserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(AuthorizationUtils.ofUserNamespace(metalake))
              .withRoleNames(Lists.newArrayList())
              .withEnabled(enabled)
              .withExternalId(externalId)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(userEntity, false /* overwritten */);
      return userEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("User {} in the metalake {} already exists", name, metalake, e);
      throw new UserAlreadyExistsException(
          "User %s in the metalake %s already exists", name, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding user {} failed in the metalake {} due to storage issues", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeUserByExternalId(String metalake, String externalId) {
    try {
      return store
          .externalIdOperations()
          .deleteByExternalId(
              AuthorizationUtils.ofUserExternalId(metalake, externalId), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user with external id {} in the metalake {} failed due to storage issues",
          externalId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  User getUserByExternalId(String metalake, String externalId) throws NoSuchUserException {
    try {
      return store
          .externalIdOperations()
          .getByExternalId(
              AuthorizationUtils.ofUserExternalId(metalake, externalId),
              Entity.EntityType.USER,
              UserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn(
          "User with external id {} does not exist in the metalake {}", externalId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_EXTERNAL_ID_DOES_NOT_EXIST_MSG, externalId, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user with external id {} failed due to storage issues", externalId, ioe);
      throw new RuntimeException(ioe);
    }
  }

  User enableUser(String metalake, String externalId) throws NoSuchUserException {
    return updateEnabledByExternalId(metalake, externalId, true);
  }

  User disableUser(String metalake, String externalId) throws NoSuchUserException {
    return updateEnabledByExternalId(metalake, externalId, false);
  }

  private User updateEnabledByExternalId(String metalake, String externalId, boolean enabled)
      throws NoSuchUserException {
    try {
      return store
          .externalIdOperations()
          .updateByExternalId(
              AuthorizationUtils.ofUserExternalId(metalake, externalId),
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
      LOG.warn(
          "User with external id {} does not exist in the metalake {}", externalId, metalake, e);
      throw new NoSuchUserException(
          AuthorizationUtils.USER_WITH_EXTERNAL_ID_DOES_NOT_EXIST_MSG, externalId, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Updating enabled state for user with external id {} in the metalake {} failed due to"
              + " storage issues",
          externalId,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }
}
