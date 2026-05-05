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

package org.apache.gravitino.auth.local;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.auth.local.dto.IdpGroupDTO;
import org.apache.gravitino.auth.local.dto.IdpUserDTO;
import org.apache.gravitino.auth.local.store.IdpAuthenticationStore;
import org.apache.gravitino.auth.local.store.po.IdpGroupPO;
import org.apache.gravitino.auth.local.store.po.IdpUserPO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

/** Main service for built-in identity management APIs. */
public class IdpAuthenticationManager {

  private final Config config;
  private final IdGenerator idGenerator;
  private final IdpAuthenticationStore store;
  private final PasswordHasher passwordHasher;

  public static IdpAuthenticationManager fromEnvironment() {
    GravitinoEnv env = GravitinoEnv.getInstance();
    return new IdpAuthenticationManager(
        env.config(),
        env.idGenerator(),
        new IdpAuthenticationStore(env.idGenerator()),
        PasswordHasherFactory.create());
  }

  IdpAuthenticationManager(
      Config config,
      IdGenerator idGenerator,
      IdpAuthenticationStore store,
      PasswordHasher passwordHasher) {
    this.config = config;
    this.idGenerator = idGenerator;
    this.store = store;
    this.passwordHasher = passwordHasher;
  }

  public IdpUserDTO createUser(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    validatePassword(password);
    if (store.findUser(userName).isPresent()) {
      throw new UserAlreadyExistsException("Local user %s already exists", userName);
    }

    store.createUser(idGenerator.nextId(), userName, passwordHasher.hash(password), currentUser());
    return getUser(userName);
  }

  public IdpUserDTO getUser(String userName) {
    ensureBasicEnabled();
    validateUserName(userName);
    IdpUserPO userPO =
        store
            .findUser(userName)
            .orElseThrow(() -> new NoSuchUserException("Local user %s does not exist", userName));
    return toUserDTO(userPO);
  }

  public String[] listUsers() {
    ensureBasicEnabled();
    ensureServiceAdmin();
    return store.listUserNames().toArray(new String[0]);
  }

  public boolean deleteUser(String userName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    Optional<IdpUserPO> user = store.findUser(userName);
    return user.isPresent() && store.deleteUser(user.get(), currentUser());
  }

  public IdpUserDTO resetPassword(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    validatePassword(password);
    IdpUserPO userPO =
        store
            .findUser(userName)
            .orElseThrow(() -> new NoSuchUserException("Local user %s does not exist", userName));
    if (passwordHasher.verify(password, userPO.getPasswordHash())) {
      throw new PasswordUnchangedException(
          "The new password must be different from the old password");
    }

    store.updatePassword(userPO, passwordHasher.hash(password), currentUser());
    return getUser(userName);
  }

  public IdpGroupDTO createGroup(String groupName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    if (store.findGroup(groupName).isPresent()) {
      throw new GroupAlreadyExistsException("Local group %s already exists", groupName);
    }

    store.createGroup(idGenerator.nextId(), groupName, currentUser());
    return getGroup(groupName);
  }

  public IdpGroupDTO getGroup(String groupName) {
    ensureBasicEnabled();
    validateGroupName(groupName);
    IdpGroupPO groupPO =
        store
            .findGroup(groupName)
            .orElseThrow(
                () -> new NoSuchGroupException("Local group %s does not exist", groupName));
    return toGroupDTO(groupPO);
  }

  public String[] listGroups() {
    ensureBasicEnabled();
    ensureServiceAdmin();
    return store.listGroupNames().toArray(new String[0]);
  }

  public boolean deleteGroup(String groupName) {
    return deleteGroup(groupName, false);
  }

  public boolean deleteGroup(String groupName, boolean force) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    Optional<IdpGroupPO> group = store.findGroup(groupName);
    if (!group.isPresent()) {
      return false;
    }

    validateGroupDeletion(groupName, force);
    return store.deleteGroup(group.get(), currentUser());
  }

  public IdpGroupDTO addUsersToGroup(String groupName, List<String> userNames) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    store.addUsersToGroup(groupPO, users, currentUser());
    return getGroup(groupName);
  }

  public IdpGroupDTO removeUsersFromGroup(String groupName, List<String> userNames) {
    return removeUsersFromGroup(groupName, userNames, false);
  }

  public IdpGroupDTO removeUsersFromGroup(String groupName, List<String> userNames, boolean force) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    validateGroupUserRemoval(groupName, userNames, force);
    store.removeUsersFromGroup(groupPO, users, currentUser());
    return getGroup(groupName);
  }

  private IdpUserDTO toUserDTO(IdpUserPO userPO) {
    return new IdpUserDTO(userPO.getUserName(), store.listGroupNames(userPO.getUserName()));
  }

  private IdpGroupDTO toGroupDTO(IdpGroupPO groupPO) {
    return new IdpGroupDTO(groupPO.getGroupName(), store.listUserNames(groupPO.getGroupName()));
  }

  private List<IdpUserPO> requireUsers(List<String> userNames) {
    Preconditions.checkArgument(userNames != null, "Users are required");
    Set<String> normalizedUsers =
        userNames.stream()
            .peek(this::validateUserName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<IdpUserPO> users = store.findUsers(new ArrayList<>(normalizedUsers));
    if (users.size() != normalizedUsers.size()) {
      Set<String> found = users.stream().map(IdpUserPO::getUserName).collect(Collectors.toSet());
      String missingUser =
          normalizedUsers.stream().filter(name -> !found.contains(name)).findFirst().orElse("");
      throw new NoSuchUserException("Local user %s does not exist", missingUser);
    }

    return users;
  }

  private IdpGroupPO requireGroup(String groupName) {
    validateGroupName(groupName);
    return store
        .findGroup(groupName)
        .orElseThrow(() -> new NoSuchGroupException("Local group %s does not exist", groupName));
  }

  private void ensureBasicEnabled() {
    Preconditions.checkState(
        config.get(Configs.AUTHENTICATORS).contains("basic"),
        "Built-in basic authentication is disabled");
  }

  private void ensureServiceAdmin() {
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || !serviceAdmins.contains(currentUser())) {
      throw new ForbiddenException("Only Gravitino service admins can manage built-in identities");
    }
  }

  private void validateUserName(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(!userName.contains(":"), "User name cannot contain ':'");
  }

  private void validateGroupName(String groupName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name is required");
  }

  private void validatePassword(String password) {
    Preconditions.checkArgument(StringUtils.isNotBlank(password), "Password is required");
    Preconditions.checkArgument(
        password.length() >= 12, "Password length must be at least 12 characters");
    Preconditions.checkArgument(
        password.length() <= 64, "Password length must be at most 64 characters");
  }

  private void validateGroupUserRemoval(String groupName, List<String> userNames, boolean force) {
    List<String> existingUsers = store.listUserNames(groupName);
    Set<String> currentUsers = new LinkedHashSet<>(existingUsers);
    currentUsers.removeAll(new LinkedHashSet<>(userNames));
    if (!force && !existingUsers.isEmpty() && currentUsers.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing all users from local group %s is dangerous, retry with force=true if"
                  + " this is intended",
              groupName));
    }
  }

  private void validateGroupDeletion(String groupName, boolean force) {
    if (!force && !store.listUserNames(groupName).isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing local group %s is dangerous while it still has users, retry with"
                  + " force=true if this is intended",
              groupName));
    }
  }

  private String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }
}
