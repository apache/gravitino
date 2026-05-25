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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.idp;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.meta.IdpEntityType;
import org.apache.gravitino.idp.meta.IdpGroupEntity;
import org.apache.gravitino.idp.meta.IdpUserEntity;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.relational.IdpStore;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for built-in IdP users and groups. It mirrors {@link
 * org.apache.gravitino.authorization.UserGroupManager} but operates on global IdP entities.
 */
public class IdpUserGroupManager {

  private static final Logger LOG = LoggerFactory.getLogger(IdpUserGroupManager.class);

  private static final String IDP_USER_DOES_NOT_EXIST_MSG = "IdP user %s does not exist";
  private static final String IDP_GROUP_DOES_NOT_EXIST_MSG = "IdP group %s does not exist";

  private final IdpStore store;
  private final IdGenerator idGenerator;
  private final PasswordHasher passwordHasher;

  /**
   * Creates a built-in IdP user and group manager.
   *
   * @param store The entity store for built-in IdP entities.
   * @param idGenerator The id generator.
   */
  public IdpUserGroupManager(IdpStore store, IdGenerator idGenerator) {
    this(store, idGenerator, PasswordHasherFactory.create());
  }

  IdpUserGroupManager(IdpStore store, IdGenerator idGenerator, PasswordHasher passwordHasher) {
    this.store = store;
    this.idGenerator = idGenerator;
    this.passwordHasher = passwordHasher;
  }

  /**
   * Adds a built-in IdP user.
   *
   * @param username The username.
   * @param password The plaintext password.
   * @return The created built-in IdP user.
   */
  public IdpUser addUser(String username, String password) {
    try {
      IdpUserEntity userEntity =
          IdpUserEntity.builder()
              .withId(idGenerator.nextId())
              .withName(username)
              .withGroupNames(Lists.newArrayList())
              .withPasswordHash(passwordHasher.hash(password))
              .build();
      store.put(userEntity, false);
      return userEntity;
    } catch (AlreadyExistsException e) {
      LOG.warn("IdP user {} already exists", username, e);
      throw e;
    } catch (IOException ioe) {
      LOG.error("Adding IdP user {} failed due to storage issues", username, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a built-in IdP user.
   *
   * @param username The username.
   * @return True if the user was removed, false if it did not exist.
   */
  public boolean removeUser(String username) {
    try {
      return store.delete(username, IdpEntityType.IDP_USER);
    } catch (IOException ioe) {
      LOG.error("Removing IdP user {} failed due to storage issues", username, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a built-in IdP user.
   *
   * @param username The username.
   * @return The built-in IdP user.
   */
  public IdpUser getUser(String username) {
    try {
      return store.get(username, IdpEntityType.IDP_USER, IdpUserEntity.class);
    } catch (NotFoundException e) {
      LOG.warn("IdP user {} does not exist", username, e);
      throw new NotFoundException(IDP_USER_DOES_NOT_EXIST_MSG, username);
    } catch (IOException ioe) {
      LOG.error("Getting IdP user {} failed due to storage issues", username, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Changes the password for a built-in IdP user.
   *
   * @param username The username.
   * @param password The new plaintext password.
   * @return The updated built-in IdP user.
   */
  public IdpUser changePassword(String username, String password) {
    store.changePassword(username, passwordHasher.hash(password));
    return getUser(username);
  }

  /**
   * Adds a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The created built-in IdP group.
   */
  public IdpGroup addGroup(String groupName) {
    try {
      IdpGroupEntity groupEntity =
          IdpGroupEntity.builder()
              .withId(idGenerator.nextId())
              .withName(groupName)
              .withUsernames(Collections.emptyList())
              .build();
      store.put(groupEntity, false);
      return groupEntity;
    } catch (AlreadyExistsException e) {
      LOG.warn("IdP group {} already exists", groupName, e);
      throw e;
    } catch (IOException ioe) {
      LOG.error("Adding IdP group {} failed due to storage issues", groupName, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Removes a built-in IdP group.
   *
   * @param groupName The group name.
   * @param force Whether to force delete a non-empty group.
   * @return True if the group was removed, false if it did not exist.
   */
  public boolean removeGroup(String groupName, boolean force) {
    try {
      if (!store.exists(groupName, IdpEntityType.IDP_GROUP)) {
        return false;
      }
      if (!force && !getGroup(groupName).usernames().isEmpty()) {
        throw new IllegalStateException(
            String.format("IdP group %s is not empty, use force=true to delete it", groupName));
      }
      return store.delete(groupName, IdpEntityType.IDP_GROUP);
    } catch (IOException ioe) {
      LOG.error("Removing IdP group {} failed due to storage issues", groupName, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Gets a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The built-in IdP group.
   */
  public IdpGroup getGroup(String groupName) {
    try {
      return store.get(groupName, IdpEntityType.IDP_GROUP, IdpGroupEntity.class);
    } catch (NotFoundException e) {
      LOG.warn("IdP group {} does not exist", groupName, e);
      throw new NotFoundException(IDP_GROUP_DOES_NOT_EXIST_MSG, groupName);
    } catch (IOException ioe) {
      LOG.error("Getting IdP group {} failed due to storage issues", groupName, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Adds users to a built-in IdP group.
   *
   * @param groupName The group name.
   * @param usernames The usernames to add.
   * @return The updated built-in IdP group.
   */
  public IdpGroup addUsersToGroup(String groupName, List<String> usernames) {
    store.addUsersToGroup(groupName, usernames);
    return getGroup(groupName);
  }

  /**
   * Removes users from a built-in IdP group.
   *
   * @param groupName The group name.
   * @param usernames The usernames to remove.
   * @return The updated built-in IdP group.
   */
  public IdpGroup removeUsersFromGroup(String groupName, List<String> usernames) {
    store.removeUsersFromGroup(groupName, usernames);
    return getGroup(groupName);
  }
}
