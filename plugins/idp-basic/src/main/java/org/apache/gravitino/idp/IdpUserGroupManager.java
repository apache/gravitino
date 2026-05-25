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
package org.apache.gravitino.idp;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.model.IdpGroup;
import org.apache.gravitino.idp.model.IdpUser;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.idp.storage.po.IdpUserPO;
import org.apache.gravitino.idp.storage.relational.IdpGarbageCollector;
import org.apache.gravitino.idp.storage.relational.IdpRelationalStorage;
import org.apache.gravitino.idp.storage.service.IdpGroupMetaService;
import org.apache.gravitino.idp.storage.service.IdpUserMetaService;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.utils.POConverters;

/**
 * Manager for built-in IdP users and groups. It mirrors {@link
 * org.apache.gravitino.authorization.UserGroupManager} but operates on global IdP metadata.
 */
public class IdpUserGroupManager implements Closeable {

  private static final IdpUserMetaService USER_SERVICE = IdpUserMetaService.getInstance();
  private static final IdpGroupMetaService GROUP_SERVICE = IdpGroupMetaService.getInstance();

  private final IdpRelationalStorage relationalStorage;
  private final IdGenerator idGenerator;
  private final PasswordHasher passwordHasher;
  private final IdpGarbageCollector garbageCollector;

  /**
   * Creates a built-in IdP user and group manager.
   *
   * @param config The server configuration.
   * @param idGenerator The id generator.
   */
  public IdpUserGroupManager(Config config, IdGenerator idGenerator) {
    this.relationalStorage = new IdpRelationalStorage(config);
    this.idGenerator = idGenerator;
    this.passwordHasher = PasswordHasherFactory.create();
    this.garbageCollector = new IdpGarbageCollector(config);
    garbageCollector.start();
  }

  /**
   * Adds a built-in IdP user.
   *
   * @param username The username.
   * @param password The plaintext password.
   * @return The created built-in IdP user.
   */
  public IdpUser addUser(String username, String password) {
    if (userExists(username)) {
      throw new AlreadyExistsException("IdP user %s already exists", username);
    }

    USER_SERVICE.insertIdpUser(newUserPO(username, passwordHasher.hash(password)));
    return getUser(username);
  }

  /**
   * Removes a built-in IdP user.
   *
   * @param username The username.
   * @return True if the user was removed, false if it did not exist.
   */
  public boolean removeUser(String username) {
    if (!userExists(username)) {
      return false;
    }
    return USER_SERVICE.deleteIdpUser(username);
  }

  /**
   * Gets a built-in IdP user.
   *
   * @param username The username.
   * @return The built-in IdP user.
   */
  public IdpUser getUser(String username) {
    IdpUserPO userPO = USER_SERVICE.getIdpUserByUsername(username);
    return new IdpUser(userPO.getUsername(), USER_SERVICE.listGroupNamesByUsername(username));
  }

  /**
   * Changes the password for a built-in IdP user.
   *
   * @param username The username.
   * @param password The new plaintext password.
   * @return The updated built-in IdP user.
   */
  public IdpUser changePassword(String username, String password) {
    if (!USER_SERVICE.updateIdpUserPassword(username, passwordHasher.hash(password))) {
      throw new NotFoundException("IdP user not found: %s", username);
    }
    return getUser(username);
  }

  /**
   * Adds a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The created built-in IdP group.
   */
  public IdpGroup addGroup(String groupName) {
    if (groupExists(groupName)) {
      throw new AlreadyExistsException("IdP group %s already exists", groupName);
    }

    GROUP_SERVICE.insertIdpGroup(newGroupPO(groupName));
    return getGroup(groupName);
  }

  /**
   * Removes a built-in IdP group.
   *
   * @param groupName The group name.
   * @param force Whether to force delete a non-empty group.
   * @return True if the group was removed, false if it did not exist.
   */
  public boolean removeGroup(String groupName, boolean force) {
    if (!groupExists(groupName)) {
      return false;
    }
    return GROUP_SERVICE.deleteIdpGroup(groupName, force);
  }

  /**
   * Gets a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The built-in IdP group.
   */
  public IdpGroup getGroup(String groupName) {
    IdpGroupPO groupPO = GROUP_SERVICE.getIdpGroupByName(groupName);
    return new IdpGroup(groupPO.getGroupName(), GROUP_SERVICE.listUsernamesByGroupName(groupName));
  }

  /**
   * Adds users to a built-in IdP group.
   *
   * @param groupName The group name.
   * @param usernames The usernames to add.
   * @return The updated built-in IdP group.
   */
  public IdpGroup addUsersToGroup(String groupName, List<String> usernames) {
    GROUP_SERVICE.addUsersToGroup(groupName, usernames);
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
    GROUP_SERVICE.removeUsersFromGroup(groupName, usernames);
    return getGroup(groupName);
  }

  @Override
  public void close() throws IOException {
    garbageCollector.close();
    relationalStorage.close();
  }

  private IdpUserPO newUserPO(String username, String passwordHash) {
    return IdpUserPO.builder()
        .withUserId(idGenerator.nextId())
        .withUsername(username)
        .withPasswordHash(passwordHash)
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private IdpGroupPO newGroupPO(String groupName) {
    return IdpGroupPO.builder()
        .withGroupId(idGenerator.nextId())
        .withGroupName(groupName)
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private static boolean userExists(String username) {
    try {
      USER_SERVICE.getIdpUserByUsername(username);
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  private static boolean groupExists(String groupName) {
    try {
      GROUP_SERVICE.getIdpGroupByName(groupName);
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }
}
