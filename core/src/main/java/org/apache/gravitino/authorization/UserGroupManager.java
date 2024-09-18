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
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserGroupManager is used for adding, removing and getting users or groups from one metalake.
 * Metalake is like a concept of the organization. `AddUser` or `AddGroup` means that a user or
 * group enters an organization.
 */
class UserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(UserGroupManager.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;

  UserGroupManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  User addUser(String metalake, String name) throws UserAlreadyExistsException {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      UserEntity userEntity =
          UserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(AuthorizationUtils.ofUserNamespace(metalake))
              .withRoleNames(Lists.newArrayList())
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

  boolean removeUser(String metalake, String user) {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      return store.delete(AuthorizationUtils.ofUser(metalake, user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} in the metalake {} failed due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  User getUser(String metalake, String user) throws NoSuchUserException {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      return store.get(
          AuthorizationUtils.ofUser(metalake, user), Entity.EntityType.USER, UserEntity.class);

    } catch (NoSuchEntityException e) {
      LOG.warn("User {} does not exist in the metalake {}", user, metalake, e);
      throw new NoSuchUserException(AuthorizationUtils.USER_DOES_NOT_EXIST_MSG, user, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      GroupEntity groupEntity =
          GroupEntity.builder()
              .withId(idGenerator.nextId())
              .withName(group)
              .withNamespace(AuthorizationUtils.ofGroupNamespace(metalake))
              .withRoleNames(Collections.emptyList())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      store.put(groupEntity, false /* overwritten */);
      return groupEntity;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Group {} in the metalake {} already exists", group, metalake, e);
      throw new GroupAlreadyExistsException(
          "Group %s in the metalake %s already exists", group, metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Adding group {} failed in the metalake {} due to storage issues", group, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  boolean removeGroup(String metalake, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);
      return store.delete(AuthorizationUtils.ofGroup(metalake, group), Entity.EntityType.GROUP);
    } catch (IOException ioe) {
      LOG.error(
          "Removing group {} in the metalake {} failed due to storage issues",
          group,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  Group getGroup(String metalake, String group) {
    try {
      AuthorizationUtils.checkMetalakeExists(metalake);

      return store.get(
          AuthorizationUtils.ofGroup(metalake, group), Entity.EntityType.GROUP, GroupEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Group {} does not exist in the metalake {}", group, metalake, e);
      throw new NoSuchGroupException(AuthorizationUtils.GROUP_DOES_NOT_EXIST_MSG, group, metalake);
    } catch (IOException ioe) {
      LOG.error("Getting group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
