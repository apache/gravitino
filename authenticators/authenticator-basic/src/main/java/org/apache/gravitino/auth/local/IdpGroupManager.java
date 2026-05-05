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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import java.time.Instant;
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
import org.apache.gravitino.dto.IdpGroupDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpGroupMetaService;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;

/** Manager for built-in IdP group management APIs. */
public class IdpGroupManager {
  private static final long INITIAL_VERSION = 1L;

  private final Config config;
  private final IdGenerator idGenerator;
  private final IdpUserMetaService userMetaService;
  private final IdpGroupMetaService groupMetaService;

  public static IdpGroupManager fromEnvironment() {
    GravitinoEnv env = GravitinoEnv.getInstance();
    return new IdpGroupManager(
        env.config(),
        env.idGenerator(),
        IdpUserMetaService.getInstance(),
        IdpGroupMetaService.getInstance(),
        PasswordHasherFactory.create());
  }

  IdpGroupManager(
      Config config,
      IdGenerator idGenerator,
      IdpUserMetaService userMetaService,
      IdpGroupMetaService groupMetaService,
      PasswordHasher passwordHasher) {
    this.config = config;
    this.idGenerator = idGenerator;
    this.userMetaService = userMetaService;
    this.groupMetaService = groupMetaService;
  }

  public IdpGroupDTO createGroup(String groupName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    if (groupMetaService().findGroup(groupName).isPresent()) {
      throw new GroupAlreadyExistsException("Built-in IdP group %s already exists", groupName);
    }

    String auditInfo;
    try {
      Instant now = Instant.now();
      auditInfo =
          JsonUtils.anyFieldMapper()
              .writeValueAsString(
                  AuditInfo.builder()
                      .withCreator(currentUser())
                      .withCreateTime(now)
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(now)
                      .build());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize built-in IdP audit info", e);
    }

    groupMetaService()
        .createGroup(
            IdpGroupPO.builder()
                .withGroupId(nextId())
                .withGroupName(groupName)
                .withAuditInfo(auditInfo)
                .withCurrentVersion(INITIAL_VERSION)
                .withLastVersion(INITIAL_VERSION)
                .withDeletedAt(0L)
                .build());
    return getGroup(groupName);
  }

  public IdpGroupDTO getGroup(String groupName) {
    ensureBasicEnabled();
    validateGroupName(groupName);
    IdpGroupPO groupPO =
        groupMetaService()
            .findGroup(groupName)
            .orElseThrow(
                () -> new NoSuchGroupException("Built-in IdP group %s does not exist", groupName));
    return toGroupDTO(groupPO);
  }

  public boolean deleteGroup(String groupName) {
    return deleteGroup(groupName, false);
  }

  public boolean deleteGroup(String groupName, boolean force) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateGroupName(groupName);
    Optional<IdpGroupPO> group = groupMetaService().findGroup(groupName);
    if (!group.isPresent()) {
      return false;
    }

    validateGroupDeletion(groupName, force);
    Long deletedAt = Instant.now().toEpochMilli();
    String auditInfo;
    try {
      AuditInfo original =
          JsonUtils.anyFieldMapper().readValue(group.get().getAuditInfo(), AuditInfo.class);
      auditInfo =
          JsonUtils.anyFieldMapper()
              .writeValueAsString(
                  AuditInfo.builder()
                      .withCreator(original.creator())
                      .withCreateTime(original.createTime())
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(Instant.now())
                      .build());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize built-in IdP audit info", e);
    }
    return groupMetaService().deleteGroup(group.get(), deletedAt, auditInfo);
  }

  public IdpGroupDTO addUsersToGroup(String groupName, List<String> userNames) {
    ensureBasicEnabled();
    ensureServiceAdmin();
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
    String auditInfo;
    try {
      Instant now = Instant.now();
      auditInfo =
          JsonUtils.anyFieldMapper()
              .writeValueAsString(
                  AuditInfo.builder()
                      .withCreator(currentUser())
                      .withCreateTime(now)
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(now)
                      .build());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize built-in IdP audit info", e);
    }
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
              .withAuditInfo(auditInfo)
              .withCurrentVersion(INITIAL_VERSION)
              .withLastVersion(INITIAL_VERSION)
              .withDeletedAt(0L)
              .build());
    }

    groupMetaService().addUsersToGroup(relations);
    return getGroup(groupName);
  }

  public IdpGroupDTO removeUsersFromGroup(String groupName, List<String> userNames) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    IdpGroupPO groupPO = requireGroup(groupName);
    List<IdpUserPO> users = requireUsers(userNames);
    List<Long> userIds = users.stream().map(IdpUserPO::getUserId).collect(Collectors.toList());
    String auditInfo;
    try {
      Instant now = Instant.now();
      auditInfo =
          JsonUtils.anyFieldMapper()
              .writeValueAsString(
                  AuditInfo.builder()
                      .withCreator(currentUser())
                      .withCreateTime(now)
                      .withLastModifier(currentUser())
                      .withLastModifiedTime(now)
                      .build());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize built-in IdP audit info", e);
    }
    groupMetaService()
        .removeUsersFromGroup(
            groupPO.getGroupId(), userIds, Instant.now().toEpochMilli(), auditInfo);
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

  private void ensureBasicEnabled() {
    Preconditions.checkState(
        config.get(Configs.AUTHENTICATORS).contains("basic"),
        "Built-in IdP authentication is disabled");
  }

  private void ensureServiceAdmin() {
    List<String> serviceAdmins = config.get(Configs.SERVICE_ADMINS);
    if (serviceAdmins == null || !serviceAdmins.contains(currentUser())) {
      throw new ForbiddenException(
          "Only Gravitino service admins can manage built-in IdP identities");
    }
  }

  private void validateUserName(String userName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(userName), "User name is required");
    Preconditions.checkArgument(!userName.contains(":"), "User name cannot contain ':'");
  }

  private void validateGroupName(String groupName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(groupName), "Group name is required");
  }

  private IdpGroupDTO toGroupDTO(IdpGroupPO groupPO) {
    AuditInfo auditInfo;
    try {
      auditInfo = JsonUtils.anyFieldMapper().readValue(groupPO.getAuditInfo(), AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize built-in IdP audit info", e);
    }

    return IdpGroupDTO.builder()
        .withName(groupPO.getGroupName())
        .withUsers(groupMetaService.listUserNames(groupPO.getGroupName()))
        .withAudit(DTOConverters.toDTO(auditInfo))
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

  private String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }
}
