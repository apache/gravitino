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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.IdpUserDTO;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.PasswordHasherFactory;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.service.IdpUserMetaService;
import org.apache.gravitino.utils.PrincipalUtils;

/** Manager for built-in IdP user management APIs. */
public class IdpUserManager {
  private static final long INITIAL_VERSION = 1L;

  private final Config config;
  private final IdGenerator idGenerator;
  private final IdpUserMetaService userMetaService;
  private final PasswordHasher passwordHasher;

  public static IdpUserManager fromEnvironment() {
    return fromEnvironment(PasswordHasherFactory.create());
  }

  public static IdpUserManager fromEnvironment(PasswordHasher passwordHasher) {
    return new IdpUserManager(
        GravitinoEnv.getInstance().config(),
        GravitinoEnv.getInstance().idGenerator(),
        IdpUserMetaService.getInstance(),
        passwordHasher);
  }

  IdpUserManager(
      Config config,
      IdGenerator idGenerator,
      IdpUserMetaService userMetaService,
      PasswordHasher passwordHasher) {
    this.config = config;
    this.idGenerator = idGenerator;
    this.userMetaService = userMetaService;
    this.passwordHasher = passwordHasher;
  }

  public IdpUserDTO createUser(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
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

  public IdpUserDTO getUser(String userName) {
    ensureBasicEnabled();
    validateUserName(userName);
    IdpUserPO userPO =
        userMetaService()
            .findUser(userName)
            .orElseThrow(
                () -> new NoSuchUserException("Built-in IdP user %s does not exist", userName));
    return toUserDTO(userPO);
  }

  public boolean deleteUser(String userName) {
    ensureBasicEnabled();
    ensureServiceAdmin();
    validateUserName(userName);
    Optional<IdpUserPO> user = userMetaService().findUser(userName);
    if (!user.isPresent()) {
      return false;
    }

    return userMetaService().deleteUser(user.get(), System.currentTimeMillis());
  }

  public IdpUserDTO resetPassword(String userName, String password) {
    ensureBasicEnabled();
    ensureServiceAdmin();
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

  private long nextId() {
    return idGenerator.nextId();
  }

  private IdpUserMetaService userMetaService() {
    return userMetaService;
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

  private void validatePassword(String password) {
    Preconditions.checkArgument(StringUtils.isNotBlank(password), "Password is required");
    Preconditions.checkArgument(
        password.length() >= 12, "Password length must be at least 12 characters");
    Preconditions.checkArgument(
        password.length() <= 64, "Password length must be at most 64 characters");
  }

  private IdpUserDTO toUserDTO(IdpUserPO userPO) {
    return IdpUserDTO.builder()
        .withName(userPO.getUserName())
        .withGroups(userMetaService().listGroupNames(userPO.getUserName()))
        .build();
  }

  private String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }
}
