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
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.auth.BasicAuthenticator;
import org.apache.gravitino.idp.basic.IdpCredentialValidator;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
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

  private static final String BASIC_AUTHENTICATOR_CLASS_NAME =
      BasicAuthenticator.class.getCanonicalName();

  private static final IdpUserMetaService USER_SERVICE = IdpUserMetaService.getInstance();
  private static final IdpGroupMetaService GROUP_SERVICE = IdpGroupMetaService.getInstance();

  private static volatile IdpUserGroupManager instance;

  private final IdpRelationalStorage relationalStorage;
  private final IdGenerator idGenerator;
  private final PasswordHasher passwordHasher;
  private final IdpGarbageCollector garbageCollector;

  /**
   * Returns the process-wide built-in IdP user and group manager, creating it on first use.
   *
   * @param config The server configuration.
   * @param idGenerator The id generator.
   * @return The singleton {@link IdpUserGroupManager} instance.
   */
  public static IdpUserGroupManager getInstance(Config config, IdGenerator idGenerator) {
    IdpUserGroupManager local = instance;
    if (local == null) {
      synchronized (IdpUserGroupManager.class) {
        local = instance;
        if (local == null) {
          instance = new IdpUserGroupManager(config, idGenerator);
          local = instance;
        }
      }
    }
    return local;
  }

  /**
   * Closes and clears the singleton instance.
   *
   * @throws IOException if closing underlying resources fails
   */
  public static synchronized void closeInstance() throws IOException {
    if (instance != null) {
      instance.close();
      instance = null;
    }
  }

  private IdpUserGroupManager(Config config, IdGenerator idGenerator) {
    this.relationalStorage = new IdpRelationalStorage(config);
    this.idGenerator = idGenerator;
    this.passwordHasher = PasswordHasherFactory.create();
    this.garbageCollector = new IdpGarbageCollector(config);
    garbageCollector.start();
  }

  /**
   * Initializes configured service admins that do not yet exist in the built-in IdP.
   *
   * @param config The server configuration.
   * @param initialAdminPassword Initial password from {@code GRAVITINO_INITIAL_ADMIN_PASSWORD}.
   * @throws IOException if persisting a service admin fails
   */
  public void initializeConfiguredServiceAdmins(Config config, String initialAdminPassword)
      throws IOException {
    initializeConfiguredServiceAdmins(
        config, USER_SERVICE, passwordHasher, idGenerator, initialAdminPassword);
  }

  static void initializeConfiguredServiceAdmins(
      Config config,
      IdpUserMetaService userMetaService,
      PasswordHasher passwordHasher,
      IdGenerator idGenerator,
      String initialAdminPassword)
      throws IOException {
    if (!basicAuthenticatorEnabled(config)) {
      return;
    }

    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || serviceAdmins.isEmpty()) {
      return;
    }

    Map<String, String> passwordsByAdmin =
        parseInitialAdminPasswords(serviceAdmins, initialAdminPassword);
    for (String serviceAdmin : serviceAdmins) {
      IdpCredentialValidator.validateUsername(serviceAdmin);
      if (userExists(userMetaService, serviceAdmin)) {
        continue;
      }

      String password = passwordsByAdmin.get(serviceAdmin);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(password),
          "Missing initial password for configured service admin %s; declare"
              + " GRAVITINO_INITIAL_ADMIN_PASSWORD",
          serviceAdmin);
      userMetaService.insertIdpUser(
          buildUserPO(idGenerator, serviceAdmin, passwordHasher.hash(password)));
    }
  }

  /**
   * Returns whether the built-in IdP basic authenticator is enabled through {@link
   * Configs#AUTHENTICATORS}.
   *
   * @param config The server configuration.
   * @return {@code true} when {@link BasicAuthenticator} is configured
   */
  public static boolean basicAuthenticatorEnabled(Config config) {
    List<String> authenticators = config.get(Configs.AUTHENTICATORS);
    return authenticators != null && authenticators.contains(BASIC_AUTHENTICATOR_CLASS_NAME);
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
   * Authenticates a built-in IdP user with the given plaintext password.
   *
   * @param username The username.
   * @param password The plaintext password.
   * @return The authenticated user with group memberships.
   * @throws UnauthorizedException when the username or password is invalid.
   */
  public IdpUser authenticate(String username, String password) {
    try {
      IdpUserPO userPO = USER_SERVICE.getIdpUserByUsername(username);
      if (!passwordHasher.verify(password, userPO.getPasswordHash())) {
        throw new UnauthorizedException(
            "Invalid username or password", AuthConstants.AUTHORIZATION_BASIC_HEADER.trim());
      }
      return new IdpUser(username, USER_SERVICE.listGroupNamesByUsername(username));
    } catch (NotFoundException e) {
      throw new UnauthorizedException(
          "Invalid username or password", AuthConstants.AUTHORIZATION_BASIC_HEADER.trim());
    }
  }

  /**
   * Changes the password for a built-in IdP user.
   *
   * @param username The username.
   * @param password The new plaintext password.
   * @return {@code true} if the password was updated
   * @throws NotFoundException if the user does not exist
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
   * @param usersToAdd The usernames to add, or null if none.
   * @param usersToRemove The usernames to remove, or null if none.
   * @return The updated built-in IdP group.
   */
  public IdpGroup changeGroupMembership(
      String groupName, @Nullable List<String> usersToAdd, @Nullable List<String> usersToRemove) {
    List<String> usersToAddList = usersToAdd == null ? Collections.emptyList() : usersToAdd;
    List<String> usersToRemoveList =
        usersToRemove == null ? Collections.emptyList() : usersToRemove;
    Preconditions.checkArgument(
        !usersToAddList.isEmpty() || !usersToRemoveList.isEmpty(),
        "usersToAdd and usersToRemove cannot both be empty");
    GROUP_SERVICE.changeGroupMembership(groupName, usersToAddList, usersToRemoveList);
    return getGroup(groupName);
  }

  @Override
  public void close() throws IOException {
    garbageCollector.close();
    relationalStorage.close();
  }

  private IdpUserPO newUserPO(String username, String passwordHash) {
    return buildUserPO(idGenerator, username, passwordHash);
  }

  private static IdpUserPO buildUserPO(
      IdGenerator idGenerator, String username, String passwordHash) {
    return IdpUserPO.builder()
        .withUserId(idGenerator.nextId())
        .withUsername(username)
        .withPasswordHash(passwordHash)
        .withCurrentVersion(POConverters.INIT_VERSION)
        .withLastVersion(POConverters.INIT_VERSION)
        .withDeletedAt(POConverters.DEFAULT_DELETED_AT)
        .build();
  }

  private static boolean userExists(IdpUserMetaService userMetaService, String username) {
    try {
      userMetaService.getIdpUserByUsername(username);
      return true;
    } catch (NotFoundException e) {
      return false;
    }
  }

  private static Map<String, String> parseInitialAdminPasswords(
      List<String> serviceAdmins, String initialAdminPassword) {
    if (StringUtils.isBlank(initialAdminPassword)) {
      return ImmutableMap.of();
    }

    IdpCredentialValidator.validatePassword(initialAdminPassword);
    ImmutableMap.Builder<String, String> passwordsByAdmin = ImmutableMap.builder();
    for (String serviceAdmin : serviceAdmins) {
      passwordsByAdmin.put(serviceAdmin, initialAdminPassword);
    }
    return passwordsByAdmin.build();
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
