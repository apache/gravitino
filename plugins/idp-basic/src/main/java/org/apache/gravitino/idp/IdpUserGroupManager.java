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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
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
  public IdpUser addUser(String username, String password) throws IOException {
    USER_SERVICE.insertIdpUser(newUserPO(username, passwordHasher.hash(password)));
    return new IdpUser(username, Collections.emptyList());
  }

  /**
   * Removes a built-in IdP user.
   *
   * @param username The username.
   * @return True if the user was removed, false if it did not exist.
   */
  public boolean removeUser(String username) {
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
   * @return True if the password was updated, false if the user did not exist.
   */
  public boolean changePassword(String username, String password) {
    return USER_SERVICE.updateIdpUserPassword(username, passwordHasher.hash(password));
  }

  /**
   * Adds a built-in IdP group.
   *
   * @param groupName The group name.
   * @return The created built-in IdP group.
   */
  public IdpGroup addGroup(String groupName) throws IOException {
    GROUP_SERVICE.insertIdpGroup(newGroupPO(groupName));
    return new IdpGroup(groupName, Collections.emptyList());
  }

  /**
   * Removes a built-in IdP group.
   *
   * @param groupName The group name.
   * @param force Whether to force delete a non-empty group.
   * @return True if the group was removed, false if it did not exist.
   */
  public boolean removeGroup(String groupName, boolean force) {
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
   * Changes built-in IdP group membership.
   *
   * @param groupName The group name.
   * @param additions The usernames to add, or null if none.
   * @param removals The usernames to remove, or null if none.
   * @return The updated built-in IdP group.
   */
  public IdpGroup changeGroupMembership(
      String groupName, @Nullable List<String> additions, @Nullable List<String> removals) {
    List<String> additionsList = additions == null ? Collections.emptyList() : additions;
    List<String> removalsList = removals == null ? Collections.emptyList() : removals;
    Preconditions.checkArgument(
        !additionsList.isEmpty() || !removalsList.isEmpty(),
        "additions and removals cannot both be empty");
    GROUP_SERVICE.changeGroupMembership(groupName, additionsList, removalsList);
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
}
