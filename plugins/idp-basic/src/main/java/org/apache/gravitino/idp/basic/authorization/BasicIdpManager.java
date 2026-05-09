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
import org.apache.gravitino.dto.IdpGroupDTO;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;

/**
 * Built-in IdP manager implementation loaded from the {@code idp-basic} plugin.
 *
 * <p>This implementation manages both built-in IdP users and groups.
 */
public class BasicIdpManager implements IdpManager {
  private static final long INITIAL_VERSION = 1L;

  private final IdGenerator idGenerator;
  private final IdpUserMetaService userMetaService;
  private final IdpGroupMetaService groupMetaService;
  private final PasswordHasher passwordHasher;

  public BasicIdpManager() {
    this(
        GravitinoEnv.getInstance().idGenerator(),
        IdpUserMetaService.getInstance(),
        IdpGroupMetaService.getInstance(),
        PasswordHasherFactory.create());
  }

  BasicIdpManager(
      IdGenerator idGenerator,
      IdpUserMetaService userMetaService,
      IdpGroupMetaService groupMetaService,
      PasswordHasher passwordHasher) {
    this.idGenerator = idGenerator;
    this.userMetaService = userMetaService;
    this.groupMetaService = groupMetaService;
    this.passwordHasher = passwordHasher;
  }

  @Override
  public IdpUserDTO createUser(String userName, String password) {
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
    return getUser(userName);
  }

  @Override
  public IdpUserDTO getUser(String userName) {
    validateUserName(userName);
    IdpUserPO userPO =
        userMetaService()
            .findUser(userName)
            .orElseThrow(
                () -> new NoSuchUserException("Built-in IdP user %s does not exist", userName));
    return toUserDTO(userPO);
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
  public IdpUserDTO resetPassword(String userName, String password) {
    validateUserName(userName);
    validatePassword(password);
    IdpUserPO userPO =
        userMetaService()
            .findUser(userName)
            .orElseThrow(
                () -> new NoSuchUserException("Built-in IdP user %s does not exist", userName));
    if (passwordHasher.verify(password, userPO.getPasswordHash())) {
      throw new IllegalArgumentException(
          "The new password must be different from the old password");
    }

    userMetaService()
        .updatePassword(userPO, passwordHasher.hash(password), userPO.getCurrentVersion() + 1);
    return getUser(userName);
  }

  @Override
  public IdpGroupDTO createGroup(String groupName) {
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
    return getGroup(groupName);
  }

  @Override
  public IdpGroupDTO getGroup(String groupName) {
    validateGroupName(groupName);
    IdpGroupPO groupPO =
        groupMetaService()
            .findGroup(groupName)
            .orElseThrow(
                () -> new NoSuchGroupException("Built-in IdP group %s does not exist", groupName));
    return toGroupDTO(groupPO);
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
  public IdpGroupDTO addUsersToGroup(String groupName, List<String> userNames) {
    IdpGroupPO groupPO = requireGroup(groupName);
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
    return getGroup(groupName);
  }

  @Override
  public IdpGroupDTO removeUsersFromGroup(String groupName, List<String> userNames) {
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    List<Long> userIds = users.stream().map(IdpUserPO::getUserId).collect(Collectors.toList());
    groupMetaService()
        .removeUsersFromGroup(groupPO.getGroupId(), userIds, System.currentTimeMillis());
    return getGroup(groupName);
  }

  private long nextId() {
    return idGenerator.nextId();
  }

  private IdpUserMetaService userMetaService() {
    return userMetaService;
  }

  private IdpGroupMetaService groupMetaService() {
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
        .withUsers(groupMetaService.listUserNames(groupPO.getGroupName()))
        .build();
  }

  private List<IdpUserPO> requireUsers(List<String> userNames) {
    Preconditions.checkArgument(userNames != null, "Users are required");
    Set<String> normalizedUsers =
        userNames.stream()
            .peek(this::validateUserName)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<IdpUserPO> users = userMetaService.findUsers(new ArrayList<>(normalizedUsers));
    if (users.size() != normalizedUsers.size()) {
      Set<String> found = users.stream().map(IdpUserPO::getUserName).collect(Collectors.toSet());
      String missingUser =
          normalizedUsers.stream().filter(name -> !found.contains(name)).findFirst().orElse("");
      throw new NoSuchUserException("Built-in IdP user %s does not exist", missingUser);
    }

    return users;
  }

  private IdpGroupPO requireGroup(String groupName) {
    validateGroupName(groupName);
    return groupMetaService
        .findGroup(groupName)
        .orElseThrow(
            () -> new NoSuchGroupException("Built-in IdP group %s does not exist", groupName));
  }

  private void validateGroupDeletion(String groupName, boolean force) {
    if (!force && !groupMetaService.listUserNames(groupName).isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "Removing built-in IdP group %s is dangerous while it still has users, retry with"
                  + " force=true if this is intended",
              groupName));
    }
  }
}
