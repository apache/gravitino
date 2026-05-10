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
package org.apache.gravitino.idp.basic.authorization;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.IdpManager;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.idp.basic.dto.IdpGroupDTO;
import org.apache.gravitino.idp.basic.dto.IdpUserDTO;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpGroupMeta;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserMeta;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;

/**
 * Built-in IdP manager implementation backed by the {@code idp-basic} plugin.
 *
 * <p>This manager provides both the core metadata operations required by {@link IdpManager} and
 * richer DTO-oriented helpers consumed by the plugin REST resources.
 */
public class BasicIdpManager implements IdpManager {
  private static final long INITIAL_VERSION = 1L;

  private final IdGenerator idGenerator;
  private final IdpUserMetaService<IdpUserPO> userMetaService;
  private final IdpGroupMetaService<IdpGroupPO, IdpGroupUserRelPO> groupMetaService;
  private final PasswordHasher passwordHasher;

  /** Creates a manager using the runtime Gravitino environment. */
  public BasicIdpManager() {
    this(
        GravitinoEnv.getInstance().idGenerator(),
        IdpUserMetaService.getInstance(),
        IdpGroupMetaService.getInstance(),
        PasswordHasherFactory.create());
  }

  BasicIdpManager(
      IdGenerator idGenerator,
      IdpUserMetaService<IdpUserPO> userMetaService,
      IdpGroupMetaService<IdpGroupPO, IdpGroupUserRelPO> groupMetaService,
      PasswordHasher passwordHasher) {
    this.idGenerator = idGenerator;
    this.userMetaService = userMetaService;
    this.groupMetaService = groupMetaService;
    this.passwordHasher = passwordHasher;
  }

  /**
   * Creates a built-in IdP user and returns a DTO with current group membership details.
   *
   * @param userName the user name to create
   * @param password the initial password
   * @return the created user DTO
   */
  public IdpUserDTO createIdpUser(String userName, String password) {
    return toUserDTO(createUserInternal(userName, password));
  }

  /**
   * Gets a built-in IdP user with current group membership details.
   *
   * @param userName the user name
   * @return the user DTO
   */
  public IdpUserDTO getIdpUser(String userName) {
    return toUserDTO(getUserInternal(userName));
  }

  /**
   * Resets a built-in IdP user password and returns the refreshed user DTO.
   *
   * @param userName the user name
   * @param password the new password
   * @return the updated user DTO
   */
  public IdpUserDTO resetIdpUserPassword(String userName, String password) {
    return toUserDTO(resetPasswordInternal(userName, password));
  }

  /**
   * Creates a built-in IdP group and returns a DTO with its current users.
   *
   * @param groupName the group name to create
   * @return the created group DTO
   */
  public IdpGroupDTO createIdpGroup(String groupName) {
    return toGroupDTO(createGroupInternal(groupName));
  }

  /**
   * Gets a built-in IdP group with its current users.
   *
   * @param groupName the group name
   * @return the group DTO
   */
  public IdpGroupDTO getIdpGroup(String groupName) {
    return toGroupDTO(getGroupInternal(groupName));
  }

  /**
   * Adds users to a built-in IdP group and returns the refreshed group DTO.
   *
   * @param groupName the group name
   * @param userNames the users to add
   * @return the updated group DTO
   */
  public IdpGroupDTO addUsersToIdpGroup(String groupName, List<String> userNames) {
    return toGroupDTO(addUsersToGroupInternal(groupName, userNames));
  }

  /**
   * Removes users from a built-in IdP group and returns the refreshed group DTO.
   *
   * @param groupName the group name
   * @param userNames the users to remove
   * @return the updated group DTO
   */
  public IdpGroupDTO removeUsersFromIdpGroup(String groupName, List<String> userNames) {
    return toGroupDTO(removeUsersFromGroupInternal(groupName, userNames));
  }

  @Override
  public IdpUserMeta createUser(String userName, String password) {
    return createUserInternal(userName, password);
  }

  @Override
  public IdpUserMeta getUser(String userName) {
    return getUserInternal(userName);
  }

  @Override
  public boolean deleteUser(String userName) {
    validateUserName(userName);
    Optional<IdpUserPO> user = userMetaService().findUser(userName);
    if (!user.isPresent()) {
      return false;
    }

    return userMetaService().deleteUser(user.get(), System.currentTimeMillis());
  }

  @Override
  public IdpUserMeta resetPassword(String userName, String password) {
    return resetPasswordInternal(userName, password);
  }

  @Override
  public IdpGroupMeta createGroup(String groupName) {
    return createGroupInternal(groupName);
  }

  @Override
  public IdpGroupMeta getGroup(String groupName) {
    return getGroupInternal(groupName);
  }

  @Override
  public boolean deleteGroup(String groupName, boolean force) {
    validateGroupName(groupName);
    Optional<IdpGroupPO> group = groupMetaService().findGroup(groupName);
    if (!group.isPresent()) {
      return false;
    }

    validateGroupDeletion(groupName, force);
    return groupMetaService().deleteGroup(group.get(), System.currentTimeMillis());
  }

  @Override
  public IdpGroupMeta addUsersToGroup(String groupName, List<String> userNames) {
    return addUsersToGroupInternal(groupName, userNames);
  }

  @Override
  public IdpGroupMeta removeUsersFromGroup(String groupName, List<String> userNames) {
    return removeUsersFromGroupInternal(groupName, userNames);
  }

  private IdpUserPO createUserInternal(String userName, String password) {
    validateUserName(userName);
    validatePassword(password);
    if (userMetaService().findUser(userName).isPresent()) {
      throw new UserAlreadyExistsException("Built-in IdP user %s already exists", userName);
    }

    userMetaService()
        .createUser(
            IdpUserPO.builder()
                .withUserId(nextId())
                .withUserName(userName)
                .withPasswordHash(passwordHasher.hash(password))
                .withCurrentVersion(INITIAL_VERSION)
                .withLastVersion(INITIAL_VERSION)
                .withDeletedAt(0L)
                .build());
    return getUserInternal(userName);
  }

  private IdpUserPO getUserInternal(String userName) {
    validateUserName(userName);
    return userMetaService()
        .findUser(userName)
        .orElseThrow(
            () -> new NoSuchUserException("Built-in IdP user %s does not exist", userName));
  }

  private IdpUserPO resetPasswordInternal(String userName, String password) {
    validateUserName(userName);
    validatePassword(password);
    IdpUserPO userPO = getUserInternal(userName);
    if (passwordHasher.verify(password, userPO.getPasswordHash())) {
      throw new IllegalArgumentException(
          "The new password must be different from the old password");
    }

    userMetaService()
        .updatePassword(userPO, passwordHasher.hash(password), userPO.getCurrentVersion() + 1);
    return getUserInternal(userName);
  }

  private IdpGroupPO createGroupInternal(String groupName) {
    validateGroupName(groupName);
    if (groupMetaService().findGroup(groupName).isPresent()) {
      throw new GroupAlreadyExistsException("Built-in IdP group %s already exists", groupName);
    }

    groupMetaService()
        .createGroup(
            IdpGroupPO.builder()
                .withGroupId(nextId())
                .withGroupName(groupName)
                .withCurrentVersion(INITIAL_VERSION)
                .withLastVersion(INITIAL_VERSION)
                .withDeletedAt(0L)
                .build());
    return getGroupInternal(groupName);
  }

  private IdpGroupPO getGroupInternal(String groupName) {
    validateGroupName(groupName);
    return groupMetaService()
        .findGroup(groupName)
        .orElseThrow(
            () -> new NoSuchGroupException("Built-in IdP group %s does not exist", groupName));
  }

  private IdpGroupPO addUsersToGroupInternal(String groupName, List<String> userNames) {
    IdpGroupPO groupPO = getGroupInternal(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    Set<Long> requestedUserIds =
        users.stream()
            .map(IdpUserPO::getUserId)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<Long> existingUserIds =
        groupMetaService()
            .selectRelatedUserIds(groupPO.getGroupId(), new ArrayList<>(requestedUserIds));
    Set<Long> existingSet = new LinkedHashSet<>(existingUserIds);
    List<IdpGroupUserRelPO> relations = new ArrayList<>();
    for (IdpUserPO user : users) {
      if (existingSet.contains(user.getUserId())) {
        continue;
      }

      relations.add(
          IdpGroupUserRelPO.builder()
              .withId(nextId())
              .withGroupId(groupPO.getGroupId())
              .withUserId(user.getUserId())
              .withCurrentVersion(INITIAL_VERSION)
              .withLastVersion(INITIAL_VERSION)
              .withDeletedAt(0L)
              .build());
    }

    groupMetaService().addUsersToGroup(relations);
    return getGroupInternal(groupName);
  }

  private IdpGroupPO removeUsersFromGroupInternal(String groupName, List<String> userNames) {
    IdpGroupPO groupPO = getGroupInternal(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    List<Long> userIds = users.stream().map(IdpUserPO::getUserId).collect(Collectors.toList());
    groupMetaService()
        .removeUsersFromGroup(groupPO.getGroupId(), userIds, System.currentTimeMillis());
    return getGroupInternal(groupName);
  }

  private long nextId() {
    return idGenerator.nextId();
  }

  private IdpUserMetaService<IdpUserPO> userMetaService() {
    return userMetaService;
  }

  private IdpGroupMetaService<IdpGroupPO, IdpGroupUserRelPO> groupMetaService() {
    return groupMetaService;
  }

  private void validateUserName(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(!userName.contains(":"), "User name cannot contain ':'");
  }

  private void validatePassword(String password) {
    Preconditions.checkArgument(StringUtils.isNotBlank(password), "Password is required");
    Preconditions.checkArgument(
        password.length() >= 12, "Password length must be at least 12 characters");
    Preconditions.checkArgument(
        password.length() <= 64, "Password length must be at most 64 characters");
  }

  private void validateGroupName(String groupName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name is required");
  }

  private IdpUserDTO toUserDTO(IdpUserPO userPO) {
    return IdpUserDTO.builder()
        .withName(userPO.getUserName())
        .withGroups(userMetaService().listGroupNames(userPO.getUserName()))
        .build();
  }

  private IdpGroupDTO toGroupDTO(IdpGroupPO groupPO) {
    return IdpGroupDTO.builder()
        .withName(groupPO.getGroupName())
        .withUsers(groupMetaService().listUserNames(groupPO.getGroupName()))
        .build();
  }

  private List<IdpUserPO> requireUsers(List<String> userNames) {
    Preconditions.checkArgument(userNames != null, "Users are required");
    Set<String> normalizedUsers =
        userNames.stream()
            .peek(this::validateUserName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<IdpUserPO> users = userMetaService().findUsers(new ArrayList<>(normalizedUsers));
    if (users.size() != normalizedUsers.size()) {
      Set<String> found = users.stream().map(IdpUserPO::getUserName).collect(Collectors.toSet());
      String missingUser =
          normalizedUsers.stream().filter(name -> !found.contains(name)).findFirst().orElse("");
      throw new NoSuchUserException("Built-in IdP user %s does not exist", missingUser);
    }

    return users;
  }

  private void validateGroupDeletion(String groupName, boolean force) {
    if (!force && !groupMetaService().listUserNames(groupName).isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing built-in IdP group %s is dangerous while it still has users, retry with"
                  + " force=true if this is intended",
              groupName));
    }
  }
}
