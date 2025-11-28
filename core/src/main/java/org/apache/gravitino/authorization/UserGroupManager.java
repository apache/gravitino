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

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
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
  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private final EntityStore store;
  private final IdGenerator idGenerator;

  UserGroupManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  User addUser(String metalake, String name) throws UserAlreadyExistsException {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
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
      checkMetalake(NameIdentifier.of(metalake), store);
      return store.delete(AuthorizationUtils.ofUser(metalake, user), Entity.EntityType.USER);
    } catch (IOException ioe) {
      LOG.error(
          "Removing user {} in the metalake {} failed due to storage issues", user, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  User getUser(String metalake, String user) throws NoSuchUserException {
    try {
      // Only check metalake existence, not enabled status, to allow reading user info
      // on disabled metalakes for internal operations like checkCurrentUser().
      try {
        store.get(NameIdentifier.of(metalake), Entity.EntityType.METALAKE, BaseMetalake.class);
      } catch (NoSuchEntityException e) {
        LOG.error("Metalake {} does not exist", metalake, e);
        throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalake);
      }

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

  String[] listUserNames(String metalake) {

    return Arrays.stream(listUsersInternal(metalake, false /* allFields */))
        .map(User::name)
        .toArray(String[]::new);
  }

  User[] listUsers(String metalake) {
    return listUsersInternal(metalake, true /* allFields */);
  }

  Group addGroup(String metalake, String group) throws GroupAlreadyExistsException {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
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
      checkMetalake(NameIdentifier.of(metalake), store);
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
      checkMetalake(NameIdentifier.of(metalake), store);

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

  Group[] listGroups(String metalake) {
    return listGroupInternal(metalake, true);
  }

  String[] listGroupNames(String metalake) {
    return Arrays.stream(listGroupInternal(metalake, false))
        .map(Group::name)
        .toArray(String[]::new);
  }

  private User[] listUsersInternal(String metalake, boolean allFields) {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);

      Namespace namespace = AuthorizationUtils.ofUserNamespace(metalake);
      return store
          .list(namespace, UserEntity.class, Entity.EntityType.USER, allFields)
          .toArray(new User[0]);
    } catch (NoSuchEntityException e) {
      LOG.error("Metalake {} does not exist", metalake, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalake);
    } catch (IOException ioe) {
      LOG.error("Listing user under metalake {} failed due to storage issues", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private Group[] listGroupInternal(String metalake, boolean allFields) {
    try {
      checkMetalake(NameIdentifier.of(metalake), store);
      Namespace namespace = AuthorizationUtils.ofGroupNamespace(metalake);
      return store
          .list(namespace, GroupEntity.class, EntityType.GROUP, allFields)
          .toArray(new Group[0]);
    } catch (NoSuchEntityException e) {
      LOG.error("Metalake {} does not exist", metalake, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, metalake);
    } catch (IOException ioe) {
      LOG.error("Listing group under metalake {} failed due to storage issues", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }
}
