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
package org.apache.gravitino.idp.authorization;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.meta.IdpEntityType;
import org.apache.gravitino.idp.meta.IdpGroupEntity;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.relational.IdpEntityStore;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for built-in IdP users and groups. It mirrors {@link
 * org.apache.gravitino.authorization.UserGroupManager} but operates on global IdP entities.
 */
public class IdpUserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(IdpUserGroupManager.class);

  private final IdpEntityStore store;
  private final IdGenerator idGenerator;
  private final PasswordHasher passwordHasher;

  /**
   * Creates a built-in IdP user and group manager.
   *
   * @param store The entity store for built-in IdP entities.
   * @param idGenerator The id generator.
   */
  public IdpUserGroupManager(IdpEntityStore store, IdGenerator idGenerator) {
    this(store, idGenerator, PasswordHasherFactory.create());
  }

  IdpUserGroupManager(
      IdpEntityStore store, IdGenerator idGenerator, PasswordHasher passwordHasher) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.passwordHasher = passwordHasher;
  }

  /**
   * Adds a built-in IdP user.
   *
   * @param name The username.
   * @param password The plaintext password.
   * @return The created built-in IdP user.
   */
  public IdpUser addUser(String name, String password) {
    try {
      IdpUserEntity userEntity =
          IdpUserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(name)
              .withNamespace(IdpAuthorizationUtils.ofIdpUserNamespace())
              .withGroupNames(Lists.newArrayList())
              .withPasswordHash(passwordHasher.hash(password))
              .build();
      store.put(userEntity, false);
      return getUser(name);
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("IdP user {} already exists", name, e);
      throw new AlreadyExistsException("IdP user %s already exists", name);
    } catch (IOException ioe) {
      LOG.error("Adding IdP user {} failed due to storage issues", name, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a built-in IdP user.
   *
   * @param user The username.
   * @return True if the user was removed, false if it did not exist.
   */
  public boolean removeUser(String user) {
    try {
      return store.delete(IdpAuthorizationUtils.ofIdpUser(user), IdpEntityType.IDP_USER);
    } catch (IOException ioe) {
      LOG.error("Removing IdP user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a built-in IdP user.
   *
   * @param user The username.
   * @return The built-in IdP user.
   */
  public IdpUser getUser(String user) {
    try {
      return store.get(
          IdpAuthorizationUtils.ofIdpUser(user), IdpEntityType.IDP_USER, IdpUserEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("IdP user {} does not exist", user, e);
      throw new NotFoundException(IdpAuthorizationUtils.IDP_USER_DOES_NOT_EXIST_MSG, user);
    } catch (IOException ioe) {
      LOG.error("Getting IdP user {} failed due to storage issues", user, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Resets the password for a built-in IdP user.
   *
   * @param user The username.
   * @param password The new plaintext password.
   * @return The updated built-in IdP user.
   */
  public IdpUser resetPassword(String user, String password) {
    if (!IdpUserMetaService.getInstance()
        .updateIdpUserPassword(user, passwordHasher.hash(password))) {
      throw new NotFoundException(IdpAuthorizationUtils.IDP_USER_DOES_NOT_EXIST_MSG, user);
    }
    return getUser(user);
  }

  /**
   * Adds a built-in IdP group.
   *
   * @param group The group name.
   * @return The created built-in IdP group.
   */
  public IdpGroup addGroup(String group) {
    try {
      IdpGroupEntity groupEntity =
          IdpGroupEntity.builder()
              .withId(idGenerator.nextId())
              .withName(group)
              .withNamespace(IdpAuthorizationUtils.ofIdpGroupNamespace())
              .withUserNames(Collections.emptyList())
              .build();
      store.put(groupEntity, false);
      return getGroup(group);
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("IdP group {} already exists", group, e);
      throw new AlreadyExistsException("IdP group %s already exists", group);
    } catch (IOException ioe) {
      LOG.error("Adding IdP group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a built-in IdP group.
   *
   * @param group The group name.
   * @param force Whether to force delete a non-empty group.
   * @return True if the group was removed, false if it did not exist.
   */
  public boolean removeGroup(String group, boolean force) {
    if (!force) {
      List<String> members = IdpGroupMetaService.getInstance().listUsernamesByGroupName(group);
      if (!members.isEmpty()) {
        throw new IllegalStateException(
            String.format("IdP group %s is not empty, use force=true to delete it", group));
      }
    }
    try {
      return store.delete(IdpAuthorizationUtils.ofIdpGroup(group), IdpEntityType.IDP_GROUP);
    } catch (IOException ioe) {
      LOG.error("Removing IdP group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a built-in IdP group.
   *
   * @param group The group name.
   * @return The built-in IdP group.
   */
  public IdpGroup getGroup(String group) {
    try {
      return store.get(
          IdpAuthorizationUtils.ofIdpGroup(group), IdpEntityType.IDP_GROUP, IdpGroupEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("IdP group {} does not exist", group, e);
      throw new NotFoundException(IdpAuthorizationUtils.IDP_GROUP_DOES_NOT_EXIST_MSG, group);
    } catch (IOException ioe) {
      LOG.error("Getting IdP group {} failed due to storage issues", group, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Adds users to a built-in IdP group.
   *
   * @param group The group name.
   * @param usernames The usernames to add.
   * @return The updated built-in IdP group.
   */
  public IdpGroup addUsersToGroup(String group, List<String> usernames) {
    IdpGroupMetaService.getInstance().addUsersToGroup(group, usernames);
    return getGroup(group);
  }

  /**
   * Removes users from a built-in IdP group.
   *
   * @param group The group name.
   * @param usernames The usernames to remove.
   * @return The updated built-in IdP group.
   */
  public IdpGroup removeUsersFromGroup(String group, List<String> usernames) {
    IdpGroupMetaService.getInstance().removeUsersFromGroup(group, usernames);
    return getGroup(group);
  }
}
